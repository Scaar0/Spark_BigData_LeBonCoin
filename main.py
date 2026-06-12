# -*- coding: utf-8 -*-

import socket
import threading
import time
import random
import json
import os
import shutil
import http.server
from datetime import datetime, timezone

USERS    = [f"usr_{i:04d}" for i in range(1, 21)]
SELLERS  = [f"sel_{i:04d}" for i in range(1, 6)]
PRODUCTS = [f"prod_{i:04d}" for i in range(1, 11)]
CITIES   = ["Paris", "Lyon", "Marseille", "Bordeaux", "Nantes"]
CATS     = ["Véhicules", "Électronique", "Immobilier", "Mode", "Maison"]
ACTIONS  = ["AIME", "VOUT", "ACHAT"]
PRICES   = [50.0, 120.0, 450.0, 800.0, 1200.0, 2500.0, 5000.0]

OUTPUT_JSON   = "/tmp/graphe_etat.json"
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_EDGES     = 50


def generate_event():
    return json.dumps({
        "timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id":     random.choice(USERS),
        "user_city":   random.choice(CITIES),
        "product_id":  random.choice(PRODUCTS),
        "product_cat": random.choice(CATS),
        "seller_id":   random.choice(SELLERS),
        "action_type": random.choice(ACTIONS),
        "price":       random.choice(PRICES)
    })


def simulateur():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", 9990))
    s.listen(1)
    print("[Simulateur] En attente sur le port 9990...")

    conn, addr = s.accept()
    print(f"[Simulateur] PySpark connecté depuis {addr}")

    while True:
        try:
            batch = "".join(generate_event() + "\n" for _ in range(3))
            conn.send(batch.encode())
        except BrokenPipeError:
            break
        time.sleep(0.5)


def dashboard_server():
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

        def end_headers(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            super().end_headers()

        def do_GET(self):
            if self.path.startswith("/graphe_etat.json"):
                try:
                    with open(OUTPUT_JSON, "rb") as f:
                        data = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except FileNotFoundError:
                    self.send_response(404)
                    self.end_headers()
            else:
                super().do_GET()

        def log_message(self, *args):
            pass

    httpd = http.server.HTTPServer(("localhost", 8080), Handler)
    print("[Dashboard] http://localhost:8080/dashboard.html")
    httpd.serve_forever()


def run_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col, window, count, to_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    from graphframes import GraphFrame

    for cp in ["/tmp/checkpoint_main", "/tmp/checkpoints_leboncoin"]:
        if os.path.exists(cp):
            shutil.rmtree(cp)
            print(f"[Init] Checkpoint supprimé : {cp}")

    spark = SparkSession.builder \
        .appName("LeBonCoin_Streaming_Graphe") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints_leboncoin")

    schema = StructType([
        StructField("timestamp",   StringType(), True),
        StructField("user_id",     StringType(), True),
        StructField("user_city",   StringType(), True),
        StructField("product_id",  StringType(), True),
        StructField("product_cat", StringType(), True),
        StructField("seller_id",   StringType(), True),
        StructField("action_type", StringType(), True),
        StructField("price",       DoubleType(), True),
    ])

    raw_stream = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9990) \
        .load()

    events = raw_stream \
        .select(from_json(col("value"), schema).alias("d")).select("d.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))

    events_wm = events.withWatermark("event_time", "30 seconds")

    graph_state = {"vertices": {}, "edges": [], "product_cats": {}}

    def update_graph(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        for row in batch_df.collect():
            uid  = row["user_id"]
            sid  = row["seller_id"]
            pid  = row["product_id"]
            act  = row["action_type"]

            graph_state["product_cats"][pid] = row["product_cat"]

            graph_state["edges"].append({"src": uid, "dst": pid, "relationship": act})
            if len(graph_state["edges"]) > MAX_EDGES:
                graph_state["edges"].pop(0)

            propose = {"src": sid, "dst": pid, "relationship": "PROPOSE"}
            if propose not in graph_state["edges"]:
                graph_state["edges"].append(propose)
                if len(graph_state["edges"]) > MAX_EDGES:
                    graph_state["edges"].pop(0)

        active_ids = {e["src"] for e in graph_state["edges"]} | \
                     {e["dst"] for e in graph_state["edges"]}

        graph_state["vertices"] = {}
        for nid in active_ids:
            if nid.startswith("usr_"):
                graph_state["vertices"][nid] = {"id": nid, "type": "U", "label": nid}
            elif nid.startswith("sel_"):
                graph_state["vertices"][nid] = {"id": nid, "type": "S", "label": nid}
            else:
                cat = graph_state["product_cats"].get(nid, "Produit")
                graph_state["vertices"][nid] = {"id": nid, "type": "P", "label": cat}

        v_list = [(v["id"], v["type"], v["label"]) for v in graph_state["vertices"].values()]
        e_list = [(e["src"], e["dst"], e["relationship"]) for e in graph_state["edges"]]

        vertices_df = spark.createDataFrame(v_list, ["id", "type", "label"])
        edges_df    = spark.createDataFrame(e_list, ["src", "dst", "relationship"])
        g = GraphFrame(vertices_df, edges_df)

        in_deg = {r["id"]: r["inDegree"] for r in g.inDegrees.collect()}

        # PageRank tous les 10 batches seulement (opération coûteuse)
        if batch_id % 10 == 0:
            try:
                pr = {r["id"]: round(r["pagerank"], 4)
                      for r in g.pageRank(resetProbability=0.15, maxIter=3).vertices.collect()}
                graph_state["last_pr"] = pr
            except Exception:
                pass
        pr = graph_state.get("last_pr", {})

        export = {
            "batch_id": batch_id,
            "vertices": [{"id": v["id"], "type": v["type"], "label": v["label"],
                          "inDegree": in_deg.get(v["id"], 0),
                          "pagerank": pr.get(v["id"], 0.0)}
                         for v in graph_state["vertices"].values()],
            "edges": graph_state["edges"],
            "stats": {
                "total_vertices": len(graph_state["vertices"]),
                "total_edges":    len(graph_state["edges"])
            }
        }

        with open(OUTPUT_JSON, "w") as f:
            json.dump(export, f)

        print(f"[Batch {batch_id}] {export['stats']['total_vertices']} nœuds, "
              f"{export['stats']['total_edges']} arêtes")

    query = events_wm \
        .writeStream \
        .foreachBatch(update_graph) \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint_main") \
        .start()

    print("[PySpark] Streaming démarré.")
    query.awaitTermination()


if __name__ == "__main__":
    print("=" * 50)
    print("  LeBonCoin - Streaming Graphe Temps Réel")
    print("=" * 50)

    t1 = threading.Thread(target=simulateur, daemon=True)
    t1.start()
    time.sleep(1)

    t2 = threading.Thread(target=dashboard_server, daemon=True)
    t2.start()
    time.sleep(0.5)

    run_spark()

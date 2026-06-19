# 🔗 LeBonCoin - Streaming Graphe Temps Réel
> Projet Big Data - Architecture PySpark Streaming & Visualisation Dynamique de Graphes

---

## 📋 Présentation

Ce projet simule une plateforme d'annonces type **LeBonCoin** générant un flux infini d'événements utilisateurs (AIME, VOUT, ACHAT). Ces événements sont traités en temps réel via **PySpark Structured Streaming**, modélisés sous forme de **graphe de connexions** avec **GraphFrames**, puis visualisés dans un **dashboard web** se rafraîchissant toutes les 5 secondes.

### Pipeline de données

```
Simulateur (Socket) ──► PySpark Structured Streaming ──► GraphFrames ──► Dashboard HTML
     :9990                   (micro-batches 5s)          (inDegree +       :8080
                                                          PageRank)
```

---

## 🛠️ Prérequis

### Système

| Outil | Version minimale | Vérification |
|-------|-----------------|--------------|
| Python | 3.10+           | `python --version` |
| Java (JDK) | 11 ou 17        | `java -version` |
| Apache Spark | 3.5.x           | `spark-submit --version` |
| Git |                 | `git --version` |

> ⚠️ **Java est obligatoire** pour faire tourner Spark. OpenJDK 11 ou 17 est recommandé.

### Variables d'environnement requises

Vérifier que les variables suivantes sont bien définies dans votre shell (`~/.bashrc` ou `~/.zshrc`) :

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64   # adapter selon votre install
export SPARK_HOME=/opt/spark                            # adapter selon votre install
export PATH=$PATH:$SPARK_HOME/bin
```

---

## 📦 Installation

### 1. Cloner le dépôt

```bash
git clone <url-du-repo>
cd <nom-du-dossier>
```

### 2. Créer et activer un environnement virtuel Python

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1
```

### 3. Installer les dépendances Python

```bash
pip install pyspark==3.5.0 graphframes
```

> 💡 Le package **graphframes** se télécharge automatiquement au premier lancement via la configuration `spark.jars.packages` définie dans `main.py`. Une connexion internet est nécessaire au premier démarrage.

---

## 🚀 Lancement

### Démarrer l'application complète

```bash
python main.py
```

Cette commande lance **simultanément** :

1. **Le simulateur** : génère des événements JSON sur le port `9990`
2. **Le serveur dashboard** : sert l'interface web sur `http://localhost:8080`
3. **PySpark** : consomme le flux, calcule le graphe et écrit `/tmp/graphe_etat.json`

### Accéder au dashboard

Ouvrir un navigateur et aller à :

```
http://localhost:8080/dashboard.html
```

Le graphe apparaît dès le premier micro-batch (environ 5 secondes après le démarrage).

---

## 📁 Structure du projet

```
.
├── main.py            # Point d'entrée unique (simulateur + Spark + serveur HTTP)
├── dashboard.html     # Interface de visualisation du graphe (canvas + Force-directed)
└── README.md
```

### Fichiers générés à l'exécution

| Fichier | Description |
|---------|-------------|
| `/tmp/graphe_etat.json` | État courant du graphe (mis à jour à chaque batch) |
| `/tmp/checkpoint_main/` | Checkpoint Spark Structured Streaming |
| `/tmp/checkpoints_leboncoin/` | Checkpoint GraphFrames |

> Ces fichiers sont **supprimés automatiquement** au démarrage pour éviter les conflits d'état.

---

## ⚙️ Configuration

Les paramètres principaux se trouvent en haut de `main.py` :

| Paramètre | Valeur par défaut | Description |
|-----------|-------------------|-------------|
| `USERS` | 20 utilisateurs | Pool d'identifiants `usr_0001` → `usr_0020` |
| `SELLERS` | 5 vendeurs | Pool d'identifiants `sel_0001` → `sel_0005` |
| `PRODUCTS` | 10 produits | Pool d'identifiants `prod_0001` → `prod_0010` |
| `MAX_EDGES` | 50 | Nombre maximum d'arêtes conservées en mémoire |
| Port simulateur | `9990` | Socket TCP d'entrée des événements |
| Port dashboard | `8080` | Serveur HTTP du tableau de bord |
| Refresh Spark | `5 secondes` | Fréquence des micro-batches |

---

## 🧩 Architecture technique

### Composants PySpark utilisés

- **SparkSession** avec configuration mémoire et shuffle optimisés
- **Structured Streaming** en mode socket (host `localhost:9990`)
- **Schema Enforcement** strict sur les événements JSON entrants
- **Watermark** de 30 secondes pour la gestion des données en retard
- **foreachBatch** pour le traitement incrémental et la mise à jour du graphe
- **Output Mode `update`** adapté aux agrégations de graphe

### Calculs GraphFrames (par batch)

- **inDegree** : calculé à chaque batch pour tous les nœuds
- **PageRank** : calculé tous les 10 batches (opération coûteuse), `resetProbability=0.15`, `maxIter=3`

### Dashboard

Interface HTML/JS pure (canvas 2D) avec simulation de forces (*force-directed graph*) :
- Répulsion nœud-nœud (Barnes-Hut simplifié)
- Attraction par les arêtes (ressort)
- Gravité centrale
- Tooltip au survol (inDegree + PageRank)
- Rafraîchissement automatique via `fetch()` toutes les 5 secondes

---

## 🔍 Dépannage

**`java.lang.UnsatisfiedLinkError` ou Spark ne démarre pas**
→ Vérifier que `JAVA_HOME` pointe vers un JDK valide (`java -version` doit retourner 11 ou 17).

**Le dashboard reste vide (spinner infini)**
→ Vérifier que `/tmp/graphe_etat.json` est bien créé après le premier batch. Consulter les logs Spark dans le terminal.

**`ModuleNotFoundError: pyspark`**
→ S'assurer que l'environnement virtuel est bien activé (`source .venv/bin/activate`) avant de lancer `python main.py`.

**`Address already in use` sur le port 9990 ou 8080**
→ Un processus précédent tourne encore. Le tuer avec :
```bash
kill $(lsof -ti:9990)
kill $(lsof -ti:8080)
```

**Erreur GraphFrames au premier lancement**
→ Normal si Spark doit télécharger le jar. Vérifier la connexion internet et patienter. Le jar est ensuite mis en cache dans `~/.ivy2/`.

---

## 📊 Exemple de sortie console

```
==================================================
  LeBonCoin - Streaming Graphe Temps Réel
==================================================
[Simulateur] En attente sur le port 9990...
[Dashboard] http://localhost:8080/dashboard.html
[Simulateur] PySpark connecté depuis ('127.0.0.1', XXXXX)
[PySpark] Streaming démarré.
[Batch 1] 8 nœuds, 12 arêtes
[Batch 2] 14 nœuds, 27 arêtes
[Batch 3] 18 nœuds, 41 arêtes
...
```

---

## 👨‍💻 Auteur

Projet réalisé dans le cadre du module **Architecture et Programmation Distribuée Big Data**.

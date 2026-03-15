# Référence de configuration

Ce document décrit exhaustivement le système de configuration du projet :
comment il fonctionne, toutes les clés disponibles, les différences entre
environnements, et la gestion des secrets.

---

## Table des matières

1. [Principe de la configuration à deux couches](#1-principe-de-la-configuration-à-deux-couches)
2. [Référence complète du fichier YAML](#2-référence-complète-du-fichier-yaml)
3. [Référence des variables d'environnement](#3-référence-des-variables-denvironnement)
4. [Comparaison dev / staging / prod](#4-comparaison-dev--staging--prod)
5. [Gestion des secrets](#5-gestion-des-secrets)
6. [Utilisation dans le code](#6-utilisation-dans-le-code)
7. [Ajouter une nouvelle clé de configuration](#7-ajouter-une-nouvelle-clé-de-configuration)

---

## 1. Principe de la configuration à deux couches

La configuration combine deux sources, la source de priorité **haute** écrase
la source de priorité **basse** :

```
┌─────────────────────────────────────────────────────────────────┐
│  COUCHE 2 : Variables d'environnement (priorité HAUTE)          │
│                                                                 │
│  SPARK_APP_NAME=my-app                                          │
│  DATA_LAKE_PATH=s3a://bucket/lake                               │
│  SLACK_WEBHOOK_URL=https://hooks.slack.com/…                    │
│                                                                 │
│  Sources : fichier .env, shell, secrets manager injecté en CI  │
└───────────────────────────┬─────────────────────────────────────┘
                            │  os.getenv(key, yaml_value)
┌───────────────────────────▼─────────────────────────────────────┐
│  COUCHE 1 : configs/<ENV>.yaml (priorité BASSE)                 │
│                                                                 │
│  spark:                                                         │
│    app_name: spark-project                                      │
│  storage:                                                       │
│    data_lake_path: /tmp/lake                                    │
│                                                                 │
│  Sources : fichiers versionnés en git, valeurs stables          │
└─────────────────────────────────────────────────────────────────┘
```

### Résolution de l'environnement

```python
AppConfig.from_env()
    │
    ├── 1. Charge .env (sans écraser les variables shell existantes)
    ├── 2. Lit $ENV  → "dev" par défaut
    ├── 3. Charge configs/<env>.yaml
    └── 4. Applique les overrides env vars par-dessus
```

---

## 2. Référence complète du fichier YAML

### Section `spark`

```yaml
spark:
  # Nom affiché dans l'UI Spark (History Server, YARN UI)
  # Override : SPARK_APP_NAME
  app_name: spark-project                  # défaut: "spark-project"

  # URL du master Spark
  # Override : SPARK_MASTER
  # Valeurs : local[*], local[N], spark://host:7077, yarn, k8s://…
  master: local[*]                         # défaut: "local[*]"

  # Niveau de log du SparkContext (indépendant du logger Python)
  # Override : SPARK_LOG_LEVEL
  # Valeurs : ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF
  log_level: WARN                          # défaut: "WARN"

  # Nombre de partitions pour les opérations shuffle (joins, groupBy, window)
  # Règle empirique : 2-3x le nombre de cœurs totaux du cluster
  # En dev : 4 (local), en prod : 200-400 selon la taille des données
  shuffle_partitions: 200                  # défaut: 200

  # Parallélisme par défaut pour les RDD sans shuffle
  default_parallelism: 200                 # défaut: 200

  # Adaptive Query Execution : Spark optimise les plans à l'exécution
  # Désactiver uniquement si diagnostic nécessaire
  adaptive_query_execution: true           # défaut: true

  # Allocation dynamique des exécuteurs (YARN/Kubernetes uniquement)
  dynamic_allocation_enabled: false        # défaut: false

  # Configurations Spark supplémentaires (clé: valeur arbitraires)
  extra_configs:
    spark.sql.sources.partitionOverwriteMode: dynamic
    spark.databricks.delta.optimizeWrite.enabled: "true"
    spark.databricks.delta.autoCompact.enabled: "true"
    spark.dynamicAllocation.enabled: "true"           # si dynamic_allocation_enabled
    spark.dynamicAllocation.minExecutors: "2"
    spark.dynamicAllocation.maxExecutors: "50"
    spark.sql.broadcastTimeout: "600"
    spark.network.timeout: "800s"
    spark.executor.heartbeatInterval: "60s"
```

### Section `storage`

```yaml
storage:
  # Chemin racine absolu du Data Lake
  # Override : DATA_LAKE_PATH
  # Exemples :
  #   local  : /tmp/spark-project/datalake
  #   AWS S3 : s3a://your-bucket/datalake
  #   Azure  : abfss://container@account.dfs.core.windows.net/datalake
  #   GCS    : gs://your-bucket/datalake
  data_lake_path: ""                        # OBLIGATOIRE en staging/prod

  # Chemin des checkpoints pour le streaming structuré
  # Override : CHECKPOINT_PATH
  checkpoint_path: ""                       # OBLIGATOIRE si streaming utilisé

  # Noms des répertoires pour chaque zone (relatifs à data_lake_path)
  raw_zone: raw                             # défaut: "raw"
  bronze_zone: bronze                       # défaut: "bronze"
  silver_zone: silver                       # défaut: "silver"
  gold_zone: gold                           # défaut: "gold"

  # Format de stockage par défaut
  format: delta                             # défaut: "delta"

  # Mode d'écrasement des partitions
  # dynamic : écrase seulement les partitions présentes dans le DataFrame
  # static  : écrase toutes les partitions de la table
  partition_overwrite_mode: dynamic         # défaut: "dynamic"
```

### Section `data_quality`

```yaml
data_quality:
  # Active la persistance des résultats DQ dans gold/dq_results
  # Désactiver en dev pour éviter des erreurs si le path n'existe pas
  enabled: true                             # défaut: true

  # Bloque le job si au moins un check de sévérité FAIL échoue
  # false en dev pour ne pas bloquer sur des données de test imparfaites
  # true en staging/prod pour protéger la production
  fail_on_error: false                      # défaut: false

  # Taux de NULLs autorisés par défaut (utilisé dans NotNullCheck avec threshold)
  null_threshold: 0.05                      # défaut: 0.05 (5%)

  # Taux de doublons autorisés par défaut
  duplicate_threshold: 0.01                 # défaut: 0.01 (1%)

  # Chemin relatif (depuis data_lake_path) pour les résultats DQ
  results_path: data_quality_results        # défaut: "data_quality_results"
```

### Section `monitoring`

```yaml
monitoring:
  # Active le monitoring global
  enabled: true                             # défaut: true

  # Active la collecte et l'émission de métriques
  metrics_enabled: true                     # défaut: true

  # URL webhook Slack pour les alertes (laisser vide pour désactiver)
  # Override : SLACK_WEBHOOK_URL
  slack_webhook_url: ""

  # Email pour les alertes critiques
  # Override : ALERT_EMAIL
  alert_email: ""
```

---

## 3. Référence des variables d'environnement

Ces variables, si définies, écrasent les valeurs correspondantes du YAML.

### Variables de contrôle

| Variable | Description | Valeur(s) | Défaut |
|---|---|---|---|
| `ENV` | Sélectionne le fichier YAML à charger | `dev`, `staging`, `prod` | `dev` |
| `LOG_LEVEL` | Niveau du logger Python | `DEBUG`, `INFO`, `WARNING`, `ERROR` | `INFO` |
| `LOG_FORMAT` | Format des logs | `json`, `text` | `json` |

### Variables Spark

| Variable | Description | Correspond à |
|---|---|---|
| `SPARK_APP_NAME` | Nom de l'application | `spark.app_name` |
| `SPARK_MASTER` | URL du master | `spark.master` |
| `SPARK_LOG_LEVEL` | Niveau de log Spark | `spark.log_level` |

### Variables de stockage

| Variable | Description | Correspond à |
|---|---|---|
| `DATA_LAKE_PATH` | Chemin racine du Data Lake | `storage.data_lake_path` |
| `CHECKPOINT_PATH` | Chemin des checkpoints | `storage.checkpoint_path` |

### Variables de monitoring et alertes

| Variable | Description | Correspond à |
|---|---|---|
| `SLACK_WEBHOOK_URL` | URL webhook Slack | `monitoring.slack_webhook_url` |
| `ALERT_EMAIL` | Email d'alerte | `monitoring.alert_email` |

### Variables de cloud (non gérées par AppConfig — configurées directement dans Spark)

Ces variables sont lues par les connecteurs Spark/Hadoop, pas par AppConfig :

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | Clé d'accès AWS (préférer les rôles IAM en prod) |
| `AWS_SECRET_ACCESS_KEY` | Secret AWS |
| `AWS_DEFAULT_REGION` | Région AWS |
| `AZURE_STORAGE_ACCOUNT` | Compte Azure Data Lake Storage |
| `AZURE_STORAGE_KEY` | Clé d'accès ADLS |

---

## 4. Comparaison dev / staging / prod

| Paramètre | dev | staging | prod |
|---|---|---|---|
| `spark.master` | `local[*]` | `yarn` | `yarn` |
| `spark.log_level` | `INFO` | `WARN` | `WARN` |
| `spark.shuffle_partitions` | `4` | `100` | `200` |
| `spark.adaptive_query_execution` | `true` | `true` | `true` |
| `spark.dynamic_allocation_enabled` | `false` | `true` | `true` |
| `storage.data_lake_path` | `/tmp/spark-project/datalake` | `s3a://…-staging/…` | `s3a://…-prod/…` |
| `data_quality.enabled` | `true` | `true` | `true` |
| `data_quality.fail_on_error` | `false` | `true` | `true` |
| `data_quality.null_threshold` | `0.05` | `0.02` | `0.01` |
| `monitoring.enabled` | `false` | `true` | `true` |

### Pourquoi `shuffle_partitions` diffère

En dev, `local[*]` n'a qu'un seul nœud. 200 partitions pour 1 nœud = overhead massif.
4 partitions pour du développement local est beaucoup plus rapide.

En prod, le nombre optimal dépend de la taille des données et du nombre de cœurs.
Formule indicative : `shuffle_partitions = (taille_données_GB * 1024 / 128) * 2`

### Pourquoi `fail_on_error: false` en dev

En développement, les données de test peuvent être incomplètes ou artificielles.
Bloquer le pipeline sur un check DQ ralentirait trop le développement.
En staging et production, la qualité est obligatoire.

---

## 5. Gestion des secrets

### Principe

**Jamais de secrets dans le code ou les fichiers YAML committés en git.**

Les secrets sont injectés via :
- Variables d'environnement shell
- Fichier `.env` local (jamais commité — voir `.gitignore`)
- Gestionnaires de secrets en production

### Développement local

```bash
# Copier le template
cp .env.example .env

# Éditer .env et remplir les vraies valeurs
# Ce fichier est dans .gitignore → jamais commité
```

### CI/CD (GitHub Actions)

Les secrets sont définis dans **Settings → Secrets and variables → Actions** du dépôt GitHub :

```yaml
# Dans .github/workflows/ci.yml
env:
  DATA_LAKE_PATH: ${{ secrets.DATA_LAKE_PATH }}
  AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
```

### Production — AWS Secrets Manager

```python
import boto3
import json

def get_secrets(secret_name: str, region: str = "eu-west-1") -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Dans pre_run() d'un job qui nécessite des credentials
def pre_run(self) -> None:
    secrets = get_secrets("spark-project/prod/jdbc")
    os.environ["JDBC_PASSWORD"] = secrets["password"]
```

### Production — Databricks Secrets

```python
# Dans un notebook Databricks
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

jdbc_password = dbutils.secrets.get(scope="spark-project", key="jdbc-password")
```

### Production — HashiCorp Vault

```bash
# Injection via envconsul ou vault agent
vault kv get -field=password secret/spark-project/jdbc
```

---

## 6. Utilisation dans le code

### Accès aux valeurs de configuration

```python
from src.utils.config import AppConfig

config = AppConfig.from_env()

# Accès direct aux sections
config.env                               # "prod"
config.spark.app_name                    # "spark-project"
config.spark.shuffle_partitions          # 200
config.storage.data_lake_path            # "s3a://bucket/datalake"
config.data_quality.fail_on_error        # True
config.monitoring.slack_webhook_url      # "https://hooks.slack.com/…"

# Construction de chemins de zones
config.get_zone_path("silver")           # "s3a://bucket/datalake/silver"
config.get_zone_path("silver", "orders") # "s3a://bucket/datalake/silver/orders"
config.get_zone_path("gold", "dq_results/orders")
# → "s3a://bucket/datalake/gold/dq_results/orders"
```

### Dans les tests

```python
from src.utils.config import AppConfig

# Modifier la config pour un test sans toucher les fichiers
config = AppConfig()
config.storage.data_lake_path = "/tmp/test-lake"
config.data_quality.enabled = False
config.data_quality.fail_on_error = True

job = MyJob(config=config, spark=spark_fixture)
```

### Charger un fichier YAML personnalisé

```python
config = AppConfig.from_file("configs/custom_test.yaml", env="test")
```

---

## 7. Ajouter une nouvelle clé de configuration

### Exemple : ajouter un paramètre `retry_attempts`

**Étape 1 — Ajouter le champ dans le dataclass**

Dans `src/utils/config.py` :

```python
@dataclass
class DataQualityConfig:
    enabled: bool = True
    fail_on_error: bool = False
    null_threshold: float = 0.05
    duplicate_threshold: float = 0.01
    results_path: str = "data_quality_results"
    retry_attempts: int = 3             # ← nouveau champ avec valeur par défaut
```

**Étape 2 — Câbler la lecture depuis le YAML**

Toujours dans `AppConfig.from_file()` :

```python
dq_cfg = DataQualityConfig(**dq_raw) if dq_raw else DataQualityConfig()
# DataQualityConfig(**dq_raw) propage automatiquement toutes les clés
# car on utilise des dataclasses — pas de modification supplémentaire requise
```

**Étape 3 — Documenter dans les fichiers YAML**

Dans `configs/dev.yaml`, `staging.yaml`, `prod.yaml` :

```yaml
data_quality:
  enabled: true
  fail_on_error: false
  retry_attempts: 1    # dev : 1 tentative
```

**Étape 4 — Mettre à jour ce fichier de documentation**

Ajouter la clé dans la section [2. Référence complète du fichier YAML](#2-référence-complète-du-fichier-yaml).

**Étape 5 — Ajouter un test unitaire**

```python
def test_retry_attempts_loaded_from_yaml(tmp_path):
    config_data = {"data_quality": {"retry_attempts": 5}}
    config_file = tmp_path / "test.yaml"
    config_file.write_text(yaml.dump(config_data))
    config = AppConfig.from_file(config_file)
    assert config.data_quality.retry_attempts == 5
```

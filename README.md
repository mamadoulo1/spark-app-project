# spark-project

[![CI Pipeline](https://github.com/YOUR_USERNAME/spark-project/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/spark-project/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-≥80%25-brightgreen)](https://github.com/YOUR_USERNAME/spark-project/actions)
[![Python](https://img.shields.io/badge/python-3.11-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5-orange)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/delta--lake-3.1-00ADD8)](https://delta.io/)

> Framework PySpark enterprise-grade suivant l'**Architecture Médaillon** (Bronze → Silver → Gold).
> Conçu pour être maintenable, testable et déployable sur tout environnement Spark (local, YARN, Databricks, EMR).

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Structure du projet](#2-structure-du-projet)
3. [Démarrage rapide](#3-démarrage-rapide)
4. [Architecture et flux de données](#4-architecture-et-flux-de-données)
5. [Modules principaux](#5-modules-principaux)
6. [Configuration](#6-configuration)
7. [Tests](#7-tests)
8. [CI/CD et Docker](#8-cicd-et-docker)
9. [Documentation détaillée](#9-documentation-détaillée)

---

## 1. Vue d'ensemble

Ce projet fournit un **framework ETL réutilisable** qui résout les problèmes récurrents des pipelines de données en entreprise :

| Problème courant | Solution apportée |
|---|---|
| Duplication de code entre jobs | `BaseJob` centralise cycle de vie, métriques et gestion d'erreurs |
| Données corrompues en production | Moteur de qualité données (`DataQualityChecker`) avant chaque écriture |
| Schemas incohérents entre zones | Registry centralisé (`SCHEMA_REGISTRY`) avec `StructType` explicites |
| Configuration éparpillée dans le code | Config YAML par environnement + override env vars |
| Tests difficiles à écrire | Transformers stateless testables avec n'importe quel DataFrame |
| Jobs non-idempotents | Delta upsert/merge sur clé métier |

---

## 2. Structure du projet

```
spark-project/
│
├── src/                          # Code source principal
│   ├── jobs/                     # Jobs Spark exécutables
│   │   ├── base_job.py           #   Classe abstraite : cycle de vie, métriques
│   │   ├── etl_job.py            #   ETL Orders Bronze→Silver (implémentation de référence)
│   │   └── streaming_job.py      #   Ingestion Kafka→Delta en temps réel
│   │
│   ├── transformations/          # Transformers composables
│   │   ├── base_transformer.py   #   Contrat abstrait + fonction compose()
│   │   └── data_cleaner.py       #   9 transformers réutilisables
│   │
│   ├── io/                       # Lecture et écriture de données
│   │   ├── readers.py            #   Parquet, Delta, CSV, JSON, JDBC
│   │   └── writers.py            #   Delta (upsert/merge), Parquet, CSV
│   │
│   ├── quality/                  # Qualité des données
│   │   └── data_quality.py       #   Checks DQ avec sévérité WARN/FAIL
│   │
│   ├── schemas/                  # Contrats de données
│   │   └── schemas.py            #   StructType Raw/Silver/Gold + registry
│   │
│   └── utils/                    # Utilitaires transversaux
│       ├── config.py             #   Chargement config YAML + env vars
│       ├── logger.py             #   Logger JSON structuré
│       └── spark_utils.py        #   Factory SparkSession
│
├── tests/
│   ├── conftest.py               # Fixtures pytest partagées (SparkSession)
│   ├── unit/                     # Tests rapides sans I/O Spark réel
│   │   ├── test_transformations.py
│   │   ├── test_data_quality.py
│   │   └── test_config.py
│   └── integration/              # Tests end-to-end avec SparkSession local
│       └── test_etl_job.py
│
├── configs/
│   ├── dev.yaml                  # Environnement local (local[*], /tmp)
│   ├── staging.yaml              # Environnement de validation (YARN, S3 staging)
│   └── prod.yaml                 # Production (YARN, allocation dynamique)
│
├── docs/                         # Documentation technique détaillée
│   ├── architecture.md           # Patterns, diagrammes, décisions de design
│   ├── modules.md                # Référence module par module
│   ├── developer_guide.md        # Guide pour étendre le framework
│   └── configuration.md          # Référence complète de la configuration
│
├── scripts/
│   ├── submit_job.sh             # Wrapper spark-submit multi-environnements
│   └── run_tests.sh              # Runner CI avec enforcement coverage
│
├── .github/workflows/ci.yml      # Pipeline CI : lint → tests → Docker scan
├── Dockerfile                    # Image multi-stage, utilisateur non-root
├── docker-compose.yml            # Stack locale Spark + Kafka
├── Makefile                      # Raccourcis développeur
├── pyproject.toml                # Config build, pytest, black, isort, mypy
├── requirements.txt              # Dépendances production
├── requirements-dev.txt          # Dépendances dev + test
└── .env.example                  # Template des variables d'environnement
```

---

## 3. Démarrage rapide

### Prérequis

- Python 3.10+
- Java 11 ou 17 (requis par PySpark)
- Docker (optionnel, pour le cluster local)

### Installation

```bash
# 1. Créer et activer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

# 2. Installer les dépendances de développement
pip install -r requirements-dev.txt

# 3. Configurer les hooks pre-commit
pre-commit install

# 4. Copier et adapter les variables d'environnement
cp .env.example .env
```

### Lancer les tests

```bash
make test-unit          # Tests unitaires rapides (~5 secondes)
make test-integration   # Tests end-to-end avec SparkSession local
make test-coverage      # Suite complète + rapport HTML de couverture
```

### Exécuter un job localement

```python
from src.jobs.etl_job import OrdersEtlJob
from src.utils.config import AppConfig

# Utilise automatiquement configs/dev.yaml
job = OrdersEtlJob()
metrics = job.execute()
print(metrics.to_dict())
# {'elapsed_seconds': 12.4, 'rows_read': 50000, 'rows_written': 49870,
#  'rows_dropped': 130, 'dq_checks_passed': 7, 'dq_checks_failed': 0}
```

---

## 4. Architecture et flux de données

### Architecture Médaillon

```
Sources externes          Zones du Data Lake
─────────────────         ──────────────────────────────────────────────────

  Fichiers CSV/JSON  ───► RAW      (données brutes, immuables, tel quel)
  API REST           ───►          │
  Bases de données   ───► BRONZE   (données structurées, non transformées)
  Kafka (streaming)  ───►          │
                               SILVER   (données nettoyées, typées, enrichies)
                                    │
                               GOLD     (agrégats, modèles analytiques)
                                    │
                               BI / ML / API consommateurs
```

### Flux de traitement d'un job batch

```
┌─────────────────────────────────────────────────────────────────────┐
│                         BaseJob.execute()                           │
│                                                                     │
│  pre_run()                                                          │
│      │                                                              │
│      ▼                                                              │
│  run()  ◄──── Implémenté par chaque job concret                     │
│      │                                                              │
│      ├── extract()   ──► Reader  ──► DataFrame brut                 │
│      │                                                              │
│      ├── transform() ──► compose(T1, T2, T3…) ──► DataFrame propre │
│      │                                                              │
│      ├── validate()  ──► DataQualityChecker ──► PASS/WARN/FAIL      │
│      │                                                              │
│      └── load()      ──► Writer.upsert() ──► Delta Table            │
│                                                                     │
│  post_run()                                                         │
│  [on_failure() si exception]                                        │
└─────────────────────────────────────────────────────────────────────┘
```

### Flux de traitement streaming

```
Kafka Topic
    │
    ▼
readStream (format=kafka)
    │
    ▼
_parse_messages()  ──► from_json() avec schema fixe
    │
    ▼
foreachBatch ──► _write_batch()
                    │
                    ├── _transform_batch()  (TrimStrings + AddAuditColumns)
                    │
                    └── write.format("delta").mode("append")
                                │
                             Bronze Delta Table
```

---

## 5. Modules principaux

### `src/utils/config.py` — Gestion de la configuration

Charge la configuration en deux couches :
1. Fichier YAML spécifique à l'environnement (`configs/<env>.yaml`)
2. Variables d'environnement shell (priorité maximale)

```python
# Chargement automatique selon $ENV
config = AppConfig.from_env()

# Chargement explicite d'un fichier
config = AppConfig.from_file("configs/prod.yaml", env="prod")

# Construction de chemins de zones
path = config.get_zone_path("silver", "orders")
# → "s3a://bucket/silver/orders"
```

### `src/utils/logger.py` — Logger structuré

Produit des logs JSON exploitables par Datadog, Splunk, CloudWatch.

```python
logger = get_logger(__name__)
logger.info("Pipeline démarré", extra={"job_id": "etl_001", "rows": 50000})
# {"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"Pipeline démarré",
#  "job_id":"etl_001","rows":50000,"module":"etl_job","line":82}
```

### `src/schemas/schemas.py` — Contrats de données

Tous les `StructType` sont définis ici et jamais dans les jobs. L'accès se fait via le registry :

```python
from src.schemas.schemas import get_schema
schema = get_schema("silver.orders")   # Lève KeyError si absent
```

### `src/transformations/data_cleaner.py` — Transformers

Neuf transformers prêts à l'emploi, combinables avec `compose()` :

| Classe | Rôle |
|---|---|
| `DropDuplicates(subset)` | Supprime les doublons (optionnellement sur un sous-ensemble) |
| `DropNullKeys(key_columns)` | Supprime les lignes avec clés NULL |
| `CastColumns(cast_map)` | Caste les colonnes vers les types cibles |
| `RenameColumns(rename_map)` | Renomme les colonnes |
| `TrimStrings()` | Supprime les espaces en début/fin de toutes les StringType |
| `NormaliseStrings(columns, case)` | Normalise la casse (upper/lower/title) |
| `AddAuditColumns(pipeline_name, env)` | Ajoute `_processing_ts`, `_pipeline_name`, `_env` |
| `FilterRows(condition)` | Filtre par expression SQL |
| `EnforceSchema(schema)` | Sélectionne et caste selon un StructType |
| `ComputeDerivedColumns(expressions)` | Calcule de nouvelles colonnes par expressions SQL |

```python
from src.transformations.base_transformer import compose
from src.transformations.data_cleaner import TrimStrings, DropDuplicates, AddAuditColumns

pipeline = compose(
    TrimStrings(),
    DropDuplicates(subset=["order_id"]),
    AddAuditColumns(pipeline_name="orders_etl", env="prod"),
)
silver_df = pipeline(bronze_df)
```

### `src/quality/data_quality.py` — Qualité des données

Moteur DQ configurable avec deux niveaux de sévérité :
- `Severity.WARN` : le check échoue, log un warning, le job continue
- `Severity.FAIL` : le check échoue, `assert_no_failures()` lève `DataQualityError`

```python
from src.quality.data_quality import (
    DataQualityChecker, NotNullCheck, UniquenessCheck,
    AcceptedValuesCheck, RangeCheck, Severity
)

checker = (
    DataQualityChecker("silver.orders")
    .add_check(NotNullCheck("order_id"))
    .add_check(UniquenessCheck("order_id"))
    .add_check(RangeCheck("unit_price", min_val=0.0))
    .add_check(AcceptedValuesCheck("status", ["PENDING", "DELIVERED"], severity=Severity.WARN))
)
results = checker.run(df)
checker.assert_no_failures()   # lève DataQualityError si FAIL non passé
```

### `src/io/` — Lecteurs et Écrivains

Accès via la **factory** pour éviter les imports directs :

```python
from src.io.readers import create_reader
from src.io.writers import create_writer

reader = create_reader(spark, "parquet")
df = reader.read("s3a://bucket/bronze/orders/", schema=RAW_ORDERS_SCHEMA)

writer = create_writer(spark, "delta")
writer.upsert(df, "s3a://bucket/silver/orders/", merge_keys=["order_id"])
```

---

## 6. Configuration

La configuration suit une stratégie à deux couches (la couche du dessus a la priorité) :

```
Priorité haute ──► Variables d'environnement shell / .env
                          │  (SPARK_APP_NAME, DATA_LAKE_PATH, …)
                          │
Priorité basse ──► configs/<ENV>.yaml
                          │  (spark.app_name, storage.data_lake_path, …)
```

Voir [docs/configuration.md](docs/configuration.md) pour la référence complète.

---

## 7. Tests

### Stratégie

```
tests/
├── unit/          # Pas de SparkSession, mocks si nécessaire — très rapides
└── integration/   # SparkSession local[1], données en mémoire — plus lents
```

### Commandes

```bash
make test-unit          # pytest tests/unit -m unit
make test-integration   # pytest tests/integration -m integration
make test-coverage      # coverage ≥ 80 % requis (échec sinon)
```

### Fixture partagée

La `SparkSession` de test est **session-scoped** dans `tests/conftest.py` pour éviter
de recréer un contexte Spark à chaque test (coût ~5 secondes).

---

## 8. CI/CD et Docker

### Pipeline GitHub Actions (`.github/workflows/ci.yml`)

```
push / PR
    │
    ├─ quality      black ✓  isort ✓  flake8 ✓  mypy ✓
    │
    ├─ unit-tests   pytest tests/unit  coverage ≥ 80%
    │
    ├─ integration  pytest tests/integration
    │
    └─ docker-build build + Trivy scan (bloque sur CVE CRITICAL/HIGH)
```

### Docker

```bash
make docker-build   # Build image multi-stage (builder + runtime non-root)
make docker-up      # Démarre Spark Master + Worker + Kafka
make docker-down    # Arrête et supprime les volumes
```

### Makefile — Raccourcis complets

```bash
make install-dev       # Installe toutes les dépendances + pre-commit
make format            # black + isort
make lint              # flake8
make type-check        # mypy
make test              # Suite complète
make clean             # Supprime les caches, .pyc, spark-warehouse
make spark-submit-dev  # spark-submit en local[*]
make spark-submit-prod # spark-submit sur YARN cluster
```

---

## 9. Documentation détaillée

| Document | Contenu |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Patterns de design, diagrammes de dépendances, décisions d'architecture |
| [docs/modules.md](docs/modules.md) | Référence complète module par module avec exemples de code |
| [docs/developer_guide.md](docs/developer_guide.md) | Créer un nouveau job, un nouveau transformer, un nouveau check DQ |
| [docs/configuration.md](docs/configuration.md) | Toutes les clés de configuration, valeurs par défaut, différences par environnement |

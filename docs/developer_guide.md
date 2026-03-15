# Guide développeur

Ce document explique comment étendre le framework : ajouter un nouveau job,
un nouveau transformer, un nouveau check de qualité, ou un nouveau format I/O.
Il couvre également les conventions de code et le workflow de développement.

---

## Table des matières

1. [Mise en place de l'environnement](#1-mise-en-place-de-lenvironnement)
2. [Workflow quotidien](#2-workflow-quotidien)
3. [Créer un nouveau job batch](#3-créer-un-nouveau-job-batch)
4. [Créer un job streaming](#4-créer-un-job-streaming)
5. [Créer un nouveau transformer](#5-créer-un-nouveau-transformer)
6. [Créer un nouveau check de qualité](#6-créer-un-nouveau-check-de-qualité)
7. [Ajouter un nouveau format I/O](#7-ajouter-un-nouveau-format-io)
8. [Ajouter un nouveau schéma](#8-ajouter-un-nouveau-schéma)
9. [Écrire les tests](#9-écrire-les-tests)
10. [Conventions de code](#10-conventions-de-code)
11. [Déploiement](#11-déploiement)

---

## 1. Mise en place de l'environnement

### Prérequis

```bash
python --version    # Python 3.10 ou 3.11
java -version       # Java 11 ou 17 (requis par PySpark)
```

### Installation complète

```bash
# Cloner le projet
git clone <url-du-repo>
cd spark-project

# Créer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

# Installer toutes les dépendances (prod + dev + test)
pip install -r requirements-dev.txt

# Activer les hooks pre-commit (lint + format avant chaque commit)
pre-commit install

# Vérifier que tout fonctionne
make test-unit
```

### Variables d'environnement locales

```bash
cp .env.example .env
# Éditer .env et renseigner DATA_LAKE_PATH, etc.
```

---

## 2. Workflow quotidien

```bash
# 1. Formater le code avant de commiter
make format           # black + isort

# 2. Vérifier le typage
make type-check       # mypy

# 3. Lancer les tests unitaires (rapides, ~5s)
make test-unit

# 4. Lancer les tests d'intégration avant de pousser
make test-integration

# 5. Vérifier la couverture globale
make test-coverage    # doit être ≥ 80%
```

Les hooks pre-commit lancent automatiquement `black`, `isort` et `flake8`
à chaque `git commit`. Un commit qui ne passe pas le lint est bloqué.

---

## 3. Créer un nouveau job batch

### Étape 1 — Créer le fichier du job

Créer `src/jobs/customers_etl_job.py` en héritant de `BaseJob` :

```python
"""
ETL job : Bronze Customers → Silver Customers.
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from src.io.readers import create_reader
from src.io.writers import DeltaWriter
from src.jobs.base_job import BaseJob
from src.quality.data_quality import DataQualityChecker, NotNullCheck, UniquenessCheck
from src.schemas.schemas import RAW_CUSTOMERS_SCHEMA
from src.transformations.base_transformer import compose
from src.transformations.data_cleaner import (
    AddAuditColumns, CastColumns, ComputeDerivedColumns,
    DropDuplicates, DropNullKeys, TrimStrings,
)
from src.utils.config import AppConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CustomersEtlJob(BaseJob):
    """Bronze → Silver ETL pour le dataset clients."""

    JOB_NAME = "customers_etl"

    def __init__(
        self,
        config: AppConfig | None = None,
        source_path: str | None = None,
        target_path: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(config=config, **kwargs)
        self.source_path = source_path or self.config.get_zone_path("bronze", "customers")
        self.target_path = target_path or self.config.get_zone_path("silver", "customers")
        self.dq_results_path = self.config.get_zone_path("gold", "dq_results/customers")

    def extract(self) -> DataFrame:
        df = create_reader(self.spark, "parquet").read(
            self.source_path, schema=RAW_CUSTOMERS_SCHEMA
        )
        self.metrics.rows_read = df.count()
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        pipeline = compose(
            TrimStrings(),
            DropDuplicates(subset=["customer_id"]),
            DropNullKeys(key_columns=["customer_id"]),
            CastColumns({"signup_date": "date"}),
            ComputeDerivedColumns({
                "full_name": "CONCAT(INITCAP(first_name), ' ', UPPER(last_name))",
                "country_code": "UPPER(COALESCE(country, 'UNKNOWN'))",
            }),
            AddAuditColumns(pipeline_name=self.JOB_NAME, env=self.config.env),
        )
        result = pipeline(df)
        self.metrics.rows_written = result.count()
        self.metrics.rows_dropped = self.metrics.rows_read - self.metrics.rows_written
        return result

    def validate(self, df: DataFrame) -> None:
        checker = (
            DataQualityChecker(f"{self.JOB_NAME}.silver_customers")
            .add_check(NotNullCheck("customer_id"))
            .add_check(UniquenessCheck("customer_id"))
        )
        results = checker.run(df)
        self.metrics.dq_checks_passed = sum(1 for r in results if r.passed)
        self.metrics.dq_checks_failed = len(results) - self.metrics.dq_checks_passed
        if self.config.data_quality.fail_on_error:
            checker.assert_no_failures()

    def load(self, df: DataFrame) -> None:
        DeltaWriter(self.spark).upsert(
            df=df,
            path=self.target_path,
            merge_keys=["customer_id"],
        )

    def run(self) -> None:
        raw_df = self.extract()
        silver_df = self.transform(raw_df)
        self.validate(silver_df)
        self.load(silver_df)
```

### Étape 2 — Ajouter les tests d'intégration

Créer `tests/integration/test_customers_etl_job.py` :

```python
import pytest
from pyspark.sql import SparkSession
from src.jobs.customers_etl_job import CustomersEtlJob
from src.utils.config import AppConfig


@pytest.fixture
def job(spark):
    config = AppConfig()
    config.data_quality.enabled = False
    return CustomersEtlJob(config=config, spark=spark)


@pytest.mark.integration
class TestCustomersEtlJob:
    def test_transform_adds_full_name(self, spark, job):
        data = [{"customer_id": "C1", "first_name": "jean", "last_name": "dupont",
                 "email": None, "phone": None, "country": "fr", "city": None,
                 "signup_date": None, "segment": None, "_ingestion_ts": None}]
        df = spark.createDataFrame(data)
        result = job.transform(df)
        row = result.collect()[0]
        assert row["full_name"] == "Jean DUPONT"
        assert row["country_code"] == "FR"
```

### Étape 3 — Enregistrer dans le script de soumission

Dans `scripts/submit_job.sh`, le job est automatiquement résolu via le paramètre
`--job customers_etl` tant que le fichier s'appelle `src/jobs/customers_etl.py`.

```bash
./scripts/submit_job.sh --env prod --job customers_etl_job
```

---

## 4. Créer un job streaming

Pour un nouveau sujet Kafka, créer `src/jobs/products_streaming_job.py` :

```python
from src.jobs.streaming_job import OrdersStreamingJob  # réutiliser la base
from src.schemas.schemas import RAW_PRODUCTS_SCHEMA
import pyspark.sql.functions as F


class ProductsStreamingJob(OrdersStreamingJob):
    """Streaming Kafka → Delta pour les événements produits."""

    JOB_NAME = "products_streaming"

    def _parse_messages(self, raw_df):
        return (
            raw_df.select(
                F.col("offset"),
                F.col("partition"),
                F.col("timestamp").alias("kafka_ts"),
                F.from_json(F.col("value").cast("string"), RAW_PRODUCTS_SCHEMA).alias("data"),
            )
            .select("offset", "partition", "kafka_ts", "data.*")
            .withColumn("_ingestion_ts", F.col("kafka_ts"))
        )

    def _write_batch(self, batch_df, batch_id):
        target = self.config.get_zone_path("bronze", "products")
        transformed = self._transform_batch(batch_df, batch_id)
        transformed.write.format("delta").mode("append").save(target)
```

---

## 5. Créer un nouveau transformer

### Règles de conception

1. **Une classe = une transformation** atomique et nommée
2. **Stateless** : aucune dépendance à Spark, à la config ou à des fichiers externes
3. **Idempotent** si possible : appliquer deux fois = même résultat
4. **Logguer** les cas non-triviaux (nombre de lignes impactées, etc.)

### Exemple : `MaskPiiColumns`

```python
# Dans src/transformations/data_cleaner.py (ou un nouveau fichier pii.py)
import hashlib
import pyspark.sql.functions as F
from src.transformations.base_transformer import BaseTransformer


class MaskPiiColumns(BaseTransformer):
    """
    Pseudonymise les colonnes PII par hachage SHA-256.

    Remplace la valeur réelle par un hash déterministe, permettant
    de joindre les datasets tout en masquant les données personnelles.

    Args:
        columns: Liste des colonnes à pseudonymiser.
        salt: Sel cryptographique. DOIT être stocké dans un gestionnaire
              de secrets, jamais en clair dans le code.
    """

    def __init__(self, columns: list[str], salt: str = "") -> None:
        super().__init__(columns=columns)
        self.columns = columns
        self.salt = salt

    def transform(self, df):
        for col_name in self.columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.sha2(F.concat(F.col(col_name), F.lit(self.salt)), 256),
                )
        return df
```

### Ajouter les tests unitaires

```python
# Dans tests/unit/test_transformations.py

@pytest.mark.unit
class TestMaskPiiColumns:
    def test_masks_email(self, spark):
        df = spark.createDataFrame([("alice@example.com",)], ["email"])
        result = MaskPiiColumns(columns=["email"], salt="test-salt")(df)
        row = result.collect()[0]
        assert row["email"] != "alice@example.com"       # masqué
        assert len(row["email"]) == 64                    # SHA-256 hex = 64 chars

    def test_deterministic(self, spark):
        df = spark.createDataFrame([("alice@example.com",)], ["email"])
        r1 = MaskPiiColumns(["email"], "salt")(df).collect()[0]["email"]
        r2 = MaskPiiColumns(["email"], "salt")(df).collect()[0]["email"]
        assert r1 == r2   # même entrée + même sel = même hash

    def test_missing_column_ignored(self, spark):
        df = spark.createDataFrame([("A",)], ["id"])
        result = MaskPiiColumns(columns=["nonexistent"])(df)
        assert result.columns == ["id"]   # pas d'erreur, pas de modification
```

---

## 6. Créer un nouveau check de qualité

### Règles de conception

1. Hériter de `BaseCheck`
2. Implémenter `run(df, dataset) -> CheckResult`
3. Toujours retourner un `CheckResult` (pas d'exception levée)
4. Nommer le check `"<type>:<colonne>"` pour la lisibilité dans les logs

### Exemple : `RegexCheck`

```python
from src.quality.data_quality import BaseCheck, CheckResult, Severity
import pyspark.sql.functions as F


class RegexCheck(BaseCheck):
    """
    Vérifie que les valeurs d'une colonne correspondent à un pattern regex.

    Cas d'usage typiques : format email, codes postaux, SIRET, IBAN, etc.

    Args:
        column: Colonne à valider.
        pattern: Expression régulière Java (syntaxe Spark/Java).
        severity: FAIL (défaut) ou WARN.

    Example:
        RegexCheck("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        RegexCheck("postal_code", r"^\d{5}$", severity=Severity.WARN)
    """

    def __init__(
        self,
        column: str,
        pattern: str,
        severity: Severity = Severity.FAIL,
    ) -> None:
        super().__init__(f"regex:{column}", severity)
        self.column = column
        self.pattern = pattern

    def run(self, df, dataset: str) -> CheckResult:
        total = df.count()
        # Comptage des lignes NON NULL qui ne matchent pas le pattern
        failing = df.filter(
            F.col(self.column).isNotNull() &
            ~F.col(self.column).rlike(self.pattern)
        ).count()
        pass_rate = 1.0 - (failing / total if total > 0 else 0.0)
        return CheckResult(
            check_name=self.name,
            dataset=dataset,
            status="PASS" if failing == 0 else self.severity.value.upper(),
            severity=self.severity,
            total_rows=total,
            failing_rows=failing,
            passing_rows=total - failing,
            pass_rate=pass_rate,
            details={"column": self.column, "pattern": self.pattern},
        )
```

### Utilisation

```python
EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

checker = (
    DataQualityChecker("silver.customers")
    .add_check(NotNullCheck("email"))
    .add_check(RegexCheck("email", EMAIL_PATTERN, severity=Severity.WARN))
)
```

---

## 7. Ajouter un nouveau format I/O

### Exemple : `AvroReader`

```python
# Dans src/io/readers.py

class AvroReader(BaseReader):
    """Lecture de fichiers Avro avec support des schémas Confluent Schema Registry."""

    def read(
        self,
        path: str,
        schema: StructType | None = None,
        **kwargs,
    ) -> DataFrame:
        reader = self.spark.read.format("avro")
        if schema:
            reader = reader.schema(schema)
        for k, v in kwargs.items():
            reader = reader.option(k, v)
        logger.info("Reading Avro", extra={"path": path})
        return reader.load(path)
```

Ajouter l'entrée dans la factory :

```python
def create_reader(spark: SparkSession, format: str) -> BaseReader:
    registry = {
        "parquet": ParquetReader,
        "delta":   DeltaReader,
        "csv":     CsvReader,
        "json":    JsonReader,
        "jdbc":    JdbcReader,
        "avro":    AvroReader,    # ← nouvelle entrée
    }
    ...
```

---

## 8. Ajouter un nouveau schéma

### Dans `src/schemas/schemas.py`

```python
# 1. Définir le StructType
SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id",   StringType(),  nullable=False),
    StructField("product_name", StringType(),  nullable=True),
    StructField("category",     StringType(),  nullable=True),
    StructField("list_price",   DecimalType(18, 4), nullable=True),
    StructField("is_active",    BooleanType(), nullable=True),
    StructField("_processing_ts", TimestampType(), nullable=False),
])

# 2. Enregistrer dans le registry
SCHEMA_REGISTRY: dict[str, StructType] = {
    ...
    "silver.products": SILVER_PRODUCTS_SCHEMA,   # ← nouvelle entrée
}
```

### Règles de nommage

- Clé registry : `"<zone>.<dataset_snake_case>"`
- Variable : `<ZONE>_<DATASET_SNAKE_CASE>_SCHEMA`
- Toujours définir `nullable=False` pour les clés primaires
- Utiliser `DecimalType(18, 4)` pour tous les montants monétaires (jamais `DoubleType`)
- Ajouter `_ingestion_ts` en Raw/Bronze, `_processing_ts` en Silver/Gold

---

## 9. Écrire les tests

### Structure recommandée d'un fichier de test

```python
"""
Tests pour [NomDuModule].

Convention :
  - @pytest.mark.unit : pas de SparkSession externe, données en mémoire
  - @pytest.mark.integration : SparkSession local, pipeline complet
"""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from src.transformations.data_cleaner import MaskPiiColumns


@pytest.mark.unit
class TestMaskPiiColumns:
    """Grouper les tests d'une classe dans une classe de test."""

    def test_happy_path(self, spark: SparkSession) -> None:
        """Nommer les tests avec le pattern test_<scenario>_<expected_outcome>."""
        data = [("alice@example.com",)]
        df = spark.createDataFrame(data, ["email"])
        result = MaskPiiColumns(["email"])(df)
        assert "email" in result.columns

    def test_empty_dataframe(self, spark: SparkSession) -> None:
        """Toujours tester le cas limite du DataFrame vide."""
        df = spark.createDataFrame([], schema="email STRING")
        result = MaskPiiColumns(["email"])(df)
        assert result.count() == 0
```

### Bonnes pratiques de test

| À faire | À éviter |
|---|---|
| Utiliser la fixture `spark` de session scope | Créer une nouvelle SparkSession dans chaque test |
| Créer de petits DataFrames in-memory | Lire des fichiers dans les tests unitaires |
| Un `assert` par comportement testé | Accumuler 10 assertions dans un seul test |
| Tester les cas limites (vide, null, max) | Tester uniquement le happy path |
| Grouper les tests d'une classe dans une classe | Fonctions de test isolées sans structure |
| Marquer avec `@pytest.mark.unit` ou `integration` | Oublier les markers |

### Tester un job complet

```python
@pytest.mark.integration
class TestMyJobIntegration:
    @pytest.fixture
    def job(self, spark):
        config = AppConfig()
        config.data_quality.enabled = False   # désactiver la persistance DQ en test
        config.data_quality.fail_on_error = True
        return MyJob(config=config, spark=spark)   # injecter la session de test

    def test_run_does_not_raise(self, job, tmp_path):
        job.source_path = str(tmp_path / "bronze")
        job.target_path = str(tmp_path / "silver")
        # … préparer les données source …
        job.run()   # ne doit pas lever d'exception
```

---

## 10. Conventions de code

### Style général

- **Longueur de ligne :** 100 caractères maximum
- **Imports :** stdlib → third-party → src (séparés par des lignes vides, triés par isort)
- **Type hints :** obligatoires sur toutes les fonctions publiques
- **Annotations `from __future__ import annotations`** : en tête de chaque fichier

### Nommage

| Élément | Convention | Exemple |
|---|---|---|
| Classes | `PascalCase` | `OrdersEtlJob`, `NotNullCheck` |
| Fonctions/méthodes | `snake_case` | `get_logger`, `transform` |
| Constantes | `SCREAMING_SNAKE_CASE` | `VALID_ORDER_STATUSES`, `RAW_ORDERS_SCHEMA` |
| Variables privées | `_snake_case` | `self._spark`, `self._checks` |
| Attributs de classe | `SCREAMING_SNAKE_CASE` | `JOB_NAME = "orders_etl"` |
| Fichiers | `snake_case.py` | `etl_job.py`, `data_cleaner.py` |

### Docstrings

Format Google/NumPy pour toutes les classes et méthodes publiques :

```python
def read(self, path: str, schema: StructType | None = None) -> DataFrame:
    """
    Lit des fichiers Parquet depuis le chemin spécifié.

    Args:
        path: Chemin local, S3 (s3a://), ADLS (abfss://) ou GCS (gs://).
        schema: Schema explicite à appliquer. Fortement recommandé pour la
                performance (évite l'inférence de schema).

    Returns:
        DataFrame avec le schema appliqué ou inféré.

    Raises:
        AnalysisException: Si le chemin n'existe pas ou est inaccessible.
    """
```

### Logging

```python
# ✅ Bien : message court + contexte structuré dans extra={}
logger.info("Extraction terminée", extra={"rows": count, "path": path})

# ❌ Mal : message verbeux avec formatage Python
logger.info(f"Extraction terminée : {count} lignes lues depuis {path}")
```

### Gestion des erreurs

```python
# ✅ Bien : laisser les exceptions remonter ou les gérer explicitement
try:
    checker.persist_results(spark, path)
except Exception as exc:
    logger.warning("Impossible de persister les résultats DQ", extra={"error": str(exc)})
    # continuer — la persistance DQ n'est pas critique

# ❌ Mal : silencer les erreurs sans log
try:
    risky_operation()
except Exception:
    pass
```

---

## 11. Déploiement

### Via `spark-submit` (YARN / standalone)

```bash
# 1. Builder le wheel
make build   # → dist/spark_project-1.0.0-py3-none-any.whl

# 2. Soumettre en staging
./scripts/submit_job.sh --env staging --job orders_etl_job

# 3. Soumettre en production
./scripts/submit_job.sh --env prod --job orders_etl_job \
    --num-executors 20 \
    --executor-mem 8g \
    --driver-mem 4g
```

### Via Databricks

```python
# Dans un notebook Databricks
import subprocess
subprocess.run([
    "pip", "install",
    "dbfs:/packages/spark_project-1.0.0-py3-none-any.whl"
])

import os
os.environ["ENV"] = "prod"
os.environ["DATA_LAKE_PATH"] = "abfss://datalake@storage.dfs.core.windows.net"

from src.jobs.etl_job import OrdersEtlJob
metrics = OrdersEtlJob().execute()
print(metrics.to_dict())
```

### Via Docker

```bash
# Build
make docker-build

# Lancer en dev
docker run \
    -e ENV=dev \
    -e DATA_LAKE_PATH=/tmp/lake \
    -v $(pwd)/data:/tmp/lake \
    spark-project:latest \
    python -m src.jobs.etl_job
```

### Variables d'environnement requises en production

| Variable | Description | Exemple |
|---|---|---|
| `ENV` | Environnement | `prod` |
| `DATA_LAKE_PATH` | Chemin racine du Data Lake | `s3a://company-prod/datalake` |
| `CHECKPOINT_PATH` | Checkpoints streaming | `s3a://company-prod/checkpoints` |
| `SPARK_APP_NAME` | Nom dans l'UI Spark (optionnel) | `orders-etl-prod` |
| `LOG_LEVEL` | Niveau de log (optionnel) | `WARN` |
| `SLACK_WEBHOOK_URL` | Alertes (optionnel) | `https://hooks.slack.com/…` |

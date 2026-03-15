# Référence des modules

Ce document détaille chaque module du projet : son rôle, ses classes et fonctions
publiques, ses paramètres, ses comportements attendus et des exemples d'utilisation.

---

## Table des matières

1. [src/utils/logger.py](#1-srcutilsloggerpy)
2. [src/utils/config.py](#2-srcutilsconfigpy)
3. [src/utils/spark_utils.py](#3-srcutilsspark_utilspy)
4. [src/schemas/schemas.py](#4-srcschemaschemaspy)
5. [src/io/readers.py](#5-srcioreadspy)
6. [src/io/writers.py](#6-srciowriterspy)
7. [src/transformations/base_transformer.py](#7-srctransformationsbase_transformerpy)
8. [src/transformations/data_cleaner.py](#8-srctransformationsdata_cleanerpy)
9. [src/quality/data_quality.py](#9-srcqualitydata_qualitypy)
10. [src/jobs/base_job.py](#10-srcjobsbase_jobpy)
11. [src/jobs/etl_job.py](#11-srcjobsetl_jobpy)
12. [src/jobs/streaming_job.py](#12-srcjobsstreaming_jobpy)

---

## 1. `src/utils/logger.py`

**Rôle :** Fournit un logger Python standard configuré pour produire du JSON structuré,
compatible avec tous les systèmes d'agrégation de logs (Datadog, Splunk, CloudWatch, ELK).

### Classe `StructuredFormatter`

Formateur `logging.Formatter` qui sérialise chaque entrée de log en une ligne JSON
avec les champs suivants :

| Champ | Description |
|---|---|
| `timestamp` | ISO-8601 UTC |
| `level` | DEBUG / INFO / WARNING / ERROR / CRITICAL |
| `logger` | Nom du logger (chemin du module) |
| `message` | Message de log |
| `module` | Nom du fichier Python |
| `function` | Nom de la fonction appelante |
| `line` | Numéro de ligne |
| `exception` | Stack trace (si exception) |
| `...` | Tous les champs passés via `extra={}` |

### Fonction `get_logger(name, level)`

```python
def get_logger(name: str, level: str | None = None) -> logging.Logger
```

**Paramètres :**
- `name` : nom du logger, utiliser `__name__` pour le nom du module courant
- `level` : niveau de log. Par défaut : variable d'env `LOG_LEVEL`, sinon `INFO`

**Comportement :**
- Idempotent : appeler deux fois `get_logger("x")` retourne le même logger sans dupliquer les handlers
- Format contrôlé par `LOG_FORMAT` : `"json"` (défaut) ou `"text"` pour le développement local

**Variables d'environnement :**

| Variable | Valeur | Effet |
|---|---|---|
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` | Niveau de filtrage |
| `LOG_FORMAT` | `json` (défaut) | Format JSON structuré |
| `LOG_FORMAT` | `text` | Format lisible humain (`[2024-01-15 10:30:00] INFO …`) |

**Exemple :**

```python
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Log simple
logger.info("Job démarré")
# → {"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"Job démarré",…}

# Log avec contexte structuré
logger.info("Extraction terminée", extra={"rows": 50000, "source": "s3a://bucket/orders"})
# → {"timestamp":"…","level":"INFO","message":"Extraction terminée","rows":50000,"source":"s3a://…"}

# Log d'erreur avec traceback automatique
try:
    risky_operation()
except Exception:
    logger.exception("Opération échouée")
# → {"…","level":"ERROR","exception":"Traceback (most recent call last):…"}
```

---

## 2. `src/utils/config.py`

**Rôle :** Gère la configuration de l'application à travers des dataclasses typées,
chargées depuis un fichier YAML avec surcharge par variables d'environnement.

### Dataclasses de configuration

#### `SparkConfig`

| Attribut | Type | Défaut | Description |
|---|---|---|---|
| `app_name` | `str` | `"spark-project"` | Nom affiché dans l'UI Spark |
| `master` | `str` | `"local[*]"` | URL du master Spark |
| `log_level` | `str` | `"WARN"` | Niveau de log Spark |
| `shuffle_partitions` | `int` | `200` | `spark.sql.shuffle.partitions` |
| `default_parallelism` | `int` | `200` | `spark.default.parallelism` |
| `dynamic_allocation_enabled` | `bool` | `False` | Allocation dynamique des exécuteurs |
| `adaptive_query_execution` | `bool` | `True` | AQE (optimisation automatique des plans) |
| `extra_configs` | `dict[str, str]` | `{}` | Configs Spark supplémentaires |

#### `StorageConfig`

| Attribut | Type | Défaut | Description |
|---|---|---|---|
| `data_lake_path` | `str` | `""` | Chemin racine du Data Lake |
| `checkpoint_path` | `str` | `""` | Chemin des checkpoints streaming |
| `raw_zone` | `str` | `"raw"` | Nom du répertoire zone Raw |
| `bronze_zone` | `str` | `"bronze"` | Nom du répertoire zone Bronze |
| `silver_zone` | `str` | `"silver"` | Nom du répertoire zone Silver |
| `gold_zone` | `str` | `"gold"` | Nom du répertoire zone Gold |
| `format` | `str` | `"delta"` | Format de stockage par défaut |

#### `DataQualityConfig`

| Attribut | Type | Défaut | Description |
|---|---|---|---|
| `enabled` | `bool` | `True` | Active/désactive la persistance des résultats DQ |
| `fail_on_error` | `bool` | `False` | Bloque le job si un check FAIL échoue |
| `null_threshold` | `float` | `0.05` | Taux de NULLs acceptés (5 %) |
| `duplicate_threshold` | `float` | `0.01` | Taux de doublons acceptés (1 %) |
| `results_path` | `str` | `"data_quality_results"` | Chemin Delta pour les résultats DQ |

#### `MonitoringConfig`

| Attribut | Type | Défaut | Description |
|---|---|---|---|
| `enabled` | `bool` | `True` | Active le monitoring |
| `metrics_enabled` | `bool` | `True` | Active la collecte de métriques |
| `slack_webhook_url` | `str` | `""` | URL webhook Slack pour les alertes |
| `alert_email` | `str` | `""` | Email pour les alertes critiques |

### Classe `AppConfig`

#### `AppConfig.from_env(env=None)`

Méthode de fabrique principale. Lit `$ENV` pour déterminer l'environnement,
charge le YAML correspondant, puis applique les overrides env vars.

```python
# Environnement déduit de $ENV (ou "dev" par défaut)
config = AppConfig.from_env()

# Forcer un environnement spécifique
config = AppConfig.from_env(env="staging")
```

#### `AppConfig.from_file(path, env="dev")`

Charge directement un fichier YAML arbitraire. Utile pour les tests.

```python
config = AppConfig.from_file("/path/to/custom.yaml", env="test")
```

#### `AppConfig.get_zone_path(zone, dataset="")`

Construit le chemin complet d'un dataset dans une zone du Data Lake.

```python
config.storage.data_lake_path = "s3a://my-bucket"
config.storage.silver_zone = "silver"

config.get_zone_path("silver")           # → "s3a://my-bucket/silver"
config.get_zone_path("silver", "orders") # → "s3a://my-bucket/silver/orders"
config.get_zone_path("gold", "dq_results/orders")
# → "s3a://my-bucket/gold/dq_results/orders"
```

---

## 3. `src/utils/spark_utils.py`

**Rôle :** Fabrique de `SparkSession` avec configuration centralisée. Tout le code
du projet passe par cette fonction — aucun `SparkSession.builder` en dehors.

### Fonction `get_spark_session(config, testing)`

```python
def get_spark_session(
    config: AppConfig | None = None,
    *,
    testing: bool = False
) -> SparkSession
```

**Mode normal** (`testing=False`) :
- Crée une session configurée depuis `AppConfig`
- Active Delta Lake (`DeltaSparkSessionExtension`)
- Configure AQE, shuffle partitions, niveau de log
- Applique tous les `extra_configs` du YAML

**Mode test** (`testing=True`) :
- `local[1]` pour éviter le parallélisme et rendre les tests déterministes
- `shuffle_partitions=1` pour réduire le overhead
- Delta Lake activé pour tester les upserts

```python
# Production / dev
spark = get_spark_session()                     # config depuis $ENV
spark = get_spark_session(config)               # config explicite

# Tests
spark = get_spark_session(testing=True)         # local[1], minimal
```

### Fonction `stop_spark_session(spark)`

Arrête proprement la session et logue l'événement. Toujours préférer cette fonction
à `spark.stop()` directement pour le logging.

### Fonction `repartition_by_size(df, target_file_size_mb, total_size_mb)`

Repartitionne un DataFrame pour produire des fichiers proches de `target_file_size_mb` Mo.
Utile avant une écriture Parquet/Delta pour éviter les petits fichiers.

```python
# Viser des fichiers de 128 Mo sur un dataset de 5 Go
df = repartition_by_size(df, target_file_size_mb=128, total_size_mb=5120)
# → 40 partitions
```

---

## 4. `src/schemas/schemas.py`

**Rôle :** Référentiel unique de tous les schémas de données du projet.
Chaque zone et dataset a un `StructType` explicitement défini.

### Schémas disponibles

| Clé registry | Variable | Zone | Dataset |
|---|---|---|---|
| `raw.orders` | `RAW_ORDERS_SCHEMA` | Raw | Commandes brutes |
| `raw.customers` | `RAW_CUSTOMERS_SCHEMA` | Raw | Clients bruts |
| `raw.products` | `RAW_PRODUCTS_SCHEMA` | Raw | Produits bruts |
| `silver.orders` | `SILVER_ORDERS_SCHEMA` | Silver | Commandes nettoyées |
| `silver.customers` | `SILVER_CUSTOMERS_SCHEMA` | Silver | Clients enrichis |
| `gold.daily_sales` | `GOLD_DAILY_SALES_SCHEMA` | Gold | Ventes journalières |
| `gold.customer_360` | `GOLD_CUSTOMER_360_SCHEMA` | Gold | Vue 360° client |

### Différences Raw → Silver

Les schémas Raw utilisent des `StringType` pour les dates et les montants (données
telles que reçues de la source). Les schémas Silver utilisent les types natifs :

| Colonne | Raw (Bronze) | Silver |
|---|---|---|
| `order_date` | `StringType` | `DateType` |
| `unit_price` | `DoubleType` | `DecimalType(18,4)` |
| `total_amount` | absent | `DecimalType(18,4)` (calculé) |
| `_processing_ts` | absent | `TimestampType` (audit) |

### Fonction `get_schema(dataset)`

```python
from src.schemas.schemas import get_schema

schema = get_schema("silver.orders")  # retourne SILVER_ORDERS_SCHEMA
schema = get_schema("unknown")        # lève KeyError avec liste des clés valides
```

---

## 5. `src/io/readers.py`

**Rôle :** Abstraction de la lecture de données. Chaque format est encapsulé dans
une classe dédiée héritant de `BaseReader`.

### Classes disponibles

#### `ParquetReader`

```python
reader = ParquetReader(spark)
df = reader.read(
    path="s3a://bucket/bronze/orders/",
    schema=RAW_ORDERS_SCHEMA,   # optionnel mais fortement recommandé
    merge_schema=False,          # True pour des partitions au schema hétérogène
)
```

#### `DeltaReader` — avec Time Travel

```python
reader = DeltaReader(spark)

# Lecture courante
df = reader.read("s3a://bucket/silver/orders/")

# Time travel par numéro de version
df = reader.read("s3a://bucket/silver/orders/", version=42)

# Time travel par timestamp
df = reader.read("s3a://bucket/silver/orders/", timestamp="2024-01-01T00:00:00Z")
```

#### `CsvReader`

Options par défaut appliquées automatiquement :

| Option | Valeur par défaut |
|---|---|
| `header` | `True` |
| `inferSchema` | `False` (toujours spécifier un schema) |
| `sep` | `,` |
| `encoding` | `UTF-8` |
| `mode` | `PERMISSIVE` |
| `dateFormat` | `yyyy-MM-dd` |

```python
reader = CsvReader(spark)
df = reader.read(
    path="s3a://bucket/raw/orders/",
    schema=RAW_ORDERS_SCHEMA,
    sep=";",         # override de l'option par défaut
    header=True,
)
```

#### `JsonReader`

```python
reader = JsonReader(spark)
df = reader.read(
    path="s3a://bucket/raw/events/",
    schema=RAW_ORDERS_SCHEMA,
    multiline=True,   # pour des fichiers JSON multi-lignes (pas JSON Lines)
)
```

#### `JdbcReader` — avec partitionnement parallèle

```python
reader = JdbcReader(spark)
df = reader.read(
    url="jdbc:postgresql://host:5432/mydb",
    table="orders",
    user="reader_user",
    password="...",              # utiliser un gestionnaire de secrets en prod
    partition_column="order_id", # colonne numérique pour la parallélisation
    lower_bound=1,
    upper_bound=10_000_000,
    num_partitions=10,           # 10 connexions JDBC en parallèle
)
```

> **Attention :** Ne jamais écrire les mots de passe en clair dans le code.
> Utiliser AWS Secrets Manager, Azure Key Vault, HashiCorp Vault, ou les
> secrets Databricks selon l'environnement.

### Factory `create_reader(spark, format)`

Point d'entrée recommandé. Évite les imports directs des classes concrètes.

```python
from src.io.readers import create_reader

reader = create_reader(spark, "delta")
df = reader.read("s3a://bucket/silver/orders/")
```

---

## 6. `src/io/writers.py`

**Rôle :** Abstraction de l'écriture de données avec support natif des opérations
Delta Lake (append, overwrite, merge/upsert).

### `DeltaWriter.write()`

```python
writer = DeltaWriter(spark)

# Append standard
writer.write(df, "s3a://bucket/silver/orders/", mode="append")

# Overwrite complet avec repartitionnement
writer.write(
    df,
    "s3a://bucket/silver/orders/",
    mode="overwrite",
    partition_by=["order_date"],
    overwrite_schema=False,   # True seulement pour les changements breaking
    optimize_write=True,      # bin-packing automatique (Databricks)
)
```

### `DeltaWriter.upsert()` — Merge/Upsert idempotent

Méthode clé pour l'idempotence des jobs. Si la table n'existe pas, elle est créée.

```python
writer.upsert(
    df=silver_df,
    path="s3a://bucket/silver/orders/",
    merge_keys=["order_id"],           # clé(s) de correspondance source/cible
    update_columns=None,               # None = mettre à jour toutes les colonnes
    # update_columns=["status", "updated_at"],  # ou seulement certaines colonnes
    delete_condition=None,             # ex: "source.is_deleted = true"
)
```

**Logique du merge Delta :**
```sql
MERGE INTO target USING source ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *       -- ou colonnes spécifiques
WHEN NOT MATCHED THEN INSERT *
```

### Factory `create_writer(spark, format)`

```python
from src.io.writers import create_writer

writer = create_writer(spark, "delta")
writer.write(df, path, mode="append")
```

---

## 7. `src/transformations/base_transformer.py`

**Rôle :** Définit le contrat `BaseTransformer` et la fonction utilitaire `compose()`.

### Classe `BaseTransformer`

Interface minimale que tout transformer doit implémenter :

```python
class MyTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        # ... logique de transformation ...
        return df
```

**Méthode `__call__`** : délègue à `transform()` après avoir loggé le début et la fin.
Rend les instances directement appelables : `transformer(df)` au lieu de `transformer.transform(df)`.

### Fonction `compose(*transformers)`

Retourne un callable `df → df` qui applique les transformers de gauche à droite.

```python
pipeline = compose(A(), B(), C())
result = pipeline(df)
# équivalent à : result = C()(B()(A()(df)))
```

`compose()` retourne une fonction ordinaire, donc les pipelines peuvent être
stockés, passés en paramètre, ou combinés :

```python
cleaning_pipeline = compose(TrimStrings(), DropNullKeys(["id"]))
enrichment_pipeline = compose(CastColumns({"price": "decimal(18,4)"}), AddAuditColumns())

full_pipeline = compose(cleaning_pipeline, enrichment_pipeline)
```

---

## 8. `src/transformations/data_cleaner.py`

**Rôle :** Bibliothèque de transformers prêts à l'emploi couvrant les opérations
de nettoyage et d'enrichissement les plus fréquentes.

### `DropDuplicates(subset=None)`

Supprime les lignes dupliquées. Sans `subset`, compare toutes les colonnes.

```python
DropDuplicates()                          # doublons exacts sur toutes colonnes
DropDuplicates(subset=["order_id"])       # déduplique sur la clé métier
DropDuplicates(subset=["id", "version"]) # clé composite
```

### `DropNullKeys(key_columns)`

Supprime toute ligne où **au moins une** des colonnes clés est NULL.
Les clés doivent être non-nulles pour garantir l'intégrité référentielle.

```python
DropNullKeys(key_columns=["order_id", "customer_id"])
```

### `CastColumns(cast_map)`

```python
CastColumns({
    "quantity":   "integer",
    "unit_price": "decimal(18,4)",
    "order_date": "date",
    "created_at": "timestamp",
    "is_active":  "boolean",
})
```

Les colonnes absentes du DataFrame sont ignorées silencieusement (pas d'erreur).

### `RenameColumns(rename_map)`

```python
RenameColumns({
    "cust_id": "customer_id",
    "prod_id": "product_id",
    "qty":     "quantity",
})
```

### `TrimStrings()`

S'applique automatiquement à **toutes** les colonnes de type `StringType`.
Supprime les espaces, tabulations et autres caractères blancs en début/fin.

### `NormaliseStrings(columns=None, case="upper")`

```python
NormaliseStrings(case="upper")                        # toutes les StringType en MAJUSCULES
NormaliseStrings(columns=["status", "channel"])       # seulement certaines colonnes
NormaliseStrings(columns=["name"], case="title")      # "jean dupont" → "Jean Dupont"
```

### `AddAuditColumns(pipeline_name, env)`

Ajoute trois colonnes de traçabilité à la fin du DataFrame :

| Colonne | Type | Valeur |
|---|---|---|
| `_processing_ts` | `TimestampType` | UTC au moment de l'exécution |
| `_pipeline_name` | `StringType` | Valeur du paramètre `pipeline_name` |
| `_env` | `StringType` | Valeur du paramètre `env` |

```python
AddAuditColumns(pipeline_name="orders_etl", env="prod")
```

### `FilterRows(condition)`

```python
FilterRows("status != 'CANCELLED'")
FilterRows("quantity > 0 AND unit_price > 0")
FilterRows("order_date >= '2024-01-01'")
```

### `EnforceSchema(schema, strict=True)`

Sélectionne uniquement les colonnes du schéma cible et les caste vers leurs types.
Les colonnes supplémentaires du DataFrame source sont **silencieusement ignorées**.

```python
from src.schemas.schemas import SILVER_ORDERS_SCHEMA

EnforceSchema(SILVER_ORDERS_SCHEMA, strict=True)
# strict=True : lève ValueError si une colonne du schema est absente du DataFrame
# strict=False : ignore silencieusement les colonnes manquantes
```

### `ComputeDerivedColumns(expressions)`

Ajoute ou remplace des colonnes via des expressions SQL Spark.

```python
ComputeDerivedColumns({
    "total_amount":  "CAST(quantity AS DECIMAL(18,4)) * unit_price",
    "full_name":     "CONCAT(first_name, ' ', last_name)",
    "year_month":    "date_format(order_date, 'yyyy-MM')",
    "is_high_value": "total_amount > 1000",
    "currency":      "UPPER(COALESCE(currency, 'USD'))",
})
```

---

## 9. `src/quality/data_quality.py`

**Rôle :** Moteur de qualité des données configurable. Exécute des règles de validation
contre un DataFrame et produit des résultats structurés.

### Classe `DataQualityChecker`

Orchestre un ensemble de checks et collecte les résultats.

**Méthodes :**

| Méthode | Description |
|---|---|
| `add_check(check)` | Ajoute un check, retourne `self` (chaînage fluide) |
| `run(df)` | Exécute tous les checks, retourne `List[CheckResult]` |
| `persist_results(spark, path)` | Écrit les résultats en Delta (append) |
| `assert_no_failures()` | Lève `DataQualityError` si au moins un check FAIL a échoué |
| `summary` | Propriété : dict avec total/passed/failed/pass_rate |

### Checks disponibles

#### `NotNullCheck(column, severity, threshold)`

```python
NotNullCheck("order_id")                          # aucun NULL toléré (FAIL)
NotNullCheck("email", severity=Severity.WARN)     # NULLs signalés mais non bloquants
NotNullCheck("city", threshold=0.10)              # jusqu'à 10% de NULLs acceptés
```

#### `UniquenessCheck(columns, severity, threshold)`

```python
UniquenessCheck("order_id")                       # unicité stricte sur la colonne
UniquenessCheck(["order_id", "line_id"])          # unicité sur clé composite
UniquenessCheck("email", threshold=0.001)         # jusqu'à 0.1% de doublons
```

#### `AcceptedValuesCheck(column, accepted_values, severity)`

```python
AcceptedValuesCheck("status", ["PENDING", "SHIPPED", "DELIVERED"])
AcceptedValuesCheck("currency", ["USD", "EUR", "GBP"], severity=Severity.WARN)
```

#### `RowCountCheck(min_rows, max_rows, severity)`

```python
RowCountCheck(min_rows=1)                         # table non vide
RowCountCheck(min_rows=1000, max_rows=10_000_000) # plage attendue
RowCountCheck(min_rows=1, severity=Severity.WARN)
```

#### `RangeCheck(column, min_val, max_val, severity)`

```python
RangeCheck("unit_price", min_val=0.0)             # prix positifs
RangeCheck("quantity", min_val=1, max_val=9999)
RangeCheck("discount_rate", min_val=0.0, max_val=1.0)
```

### Classe `CheckResult`

Résultat d'un check individuel :

```python
result.check_name    # "not_null:order_id"
result.status        # "PASS" | "FAIL" | "WARN"
result.passed        # True/False (propriété calculée)
result.total_rows    # 50000
result.failing_rows  # 0
result.pass_rate     # 1.0
result.details       # {"column": "order_id", "null_rate": 0.0, "threshold": 0.0}
result.run_ts        # "2024-01-15T10:30:00.000Z"
result.to_dict()     # sérialisable pour Delta ou JSON
```

### `Severity` enum

```python
from src.quality.data_quality import Severity

Severity.FAIL  # →  assert_no_failures() lève DataQualityError
Severity.WARN  # →  log warning, pipeline continue
```

---

## 10. `src/jobs/base_job.py`

**Rôle :** Classe abstraite qui implémente le squelette du cycle de vie de tous les
jobs Spark. Chaque job concret hérite de `BaseJob` et n'implémente que `run()`.

### Classe `JobMetrics`

Accumulateur de métriques d'exécution :

| Attribut | Description |
|---|---|
| `elapsed_seconds` | Durée d'exécution (propriété calculée) |
| `rows_read` | Lignes extraites de la source |
| `rows_written` | Lignes écrites dans la cible |
| `rows_dropped` | Lignes éliminées (doublons, nulls, filtres) |
| `dq_checks_passed` | Nombre de checks DQ passés |
| `dq_checks_failed` | Nombre de checks DQ échoués |
| `extra` | Dict libre pour des métriques personnalisées |

### Classe `BaseJob`

**Attribut de classe :**
- `JOB_NAME: str` — identifiant du job, apparaît dans tous les logs et métriques

**Méthodes à connaître :**

```python
class MyJob(BaseJob):
    JOB_NAME = "my_job"

    def run(self) -> None:
        # OBLIGATOIRE : logique métier du job
        pass

    def pre_run(self) -> None:
        # OPTIONNEL : validation de paths, initialisation de connexions
        pass

    def post_run(self) -> None:
        # OPTIONNEL : notification succès, archivage, nettoyage
        pass

    def on_failure(self, exc: Exception) -> None:
        # OPTIONNEL : alerte Slack, PagerDuty, email
        super().on_failure(exc)  # toujours appeler le parent pour le logging
        send_slack_alert(str(exc))
```

**Accès à la SparkSession :**

```python
# Dans run(), utiliser self.spark (lazy property)
df = self.spark.read.format("delta").load(path)
```

**Accès aux métriques :**

```python
# Les métriques s'accumulent pendant run()
self.metrics.rows_read = df.count()
self.metrics.extra["custom_metric"] = 42
```

---

## 11. `src/jobs/etl_job.py`

**Rôle :** Implémentation de référence d'un job ETL batch complet.
Illustre comment orchestrer extract → transform → validate → load.

### Classe `OrdersEtlJob`

```python
job = OrdersEtlJob(
    config=AppConfig.from_env(),     # optionnel, chargé auto depuis $ENV
    source_path="s3a://bucket/bronze/orders/",  # optionnel, déduit de la config
    target_path="s3a://bucket/silver/orders/",  # optionnel, déduit de la config
)
metrics = job.execute()
```

### Pipeline de transformation

L'ordre des transformers dans `compose()` est intentionnel :

```
1. TrimStrings()
       ↓  espaces nettoyés en premier (les doublons peuvent différer avec des espaces)
2. DropDuplicates(subset=["order_id"])
       ↓  déduplication sur la clé métier propre
3. DropNullKeys(["order_id", "customer_id", "product_id"])
       ↓  suppression des lignes sans clés — avant les casts pour éviter des erreurs
4. CastColumns({"quantity": "integer", "unit_price": "decimal(18,4)", "order_date": "date"})
       ↓  typage des colonnes
5. ComputeDerivedColumns({"total_amount": "...", "currency": "UPPER(COALESCE(…))", …})
       ↓  calcul après le typage (total_amount dépend de quantity et unit_price typés)
6. AddAuditColumns(pipeline_name, env)
       ↓  colonnes d'audit ajoutées en dernier
```

### Checks de qualité appliqués

```python
RowCountCheck(min_rows=1)                          # WARN : table non vide
NotNullCheck("order_id")                           # FAIL : clé primaire
NotNullCheck("customer_id")                        # FAIL : clé étrangère
NotNullCheck("order_date")                         # FAIL : date obligatoire
NotNullCheck("total_amount")                       # FAIL : montant calculé
UniquenessCheck("order_id")                        # FAIL : unicité PK
AcceptedValuesCheck("status", VALID_STATUSES)      # WARN : statuts connus
```

---

## 12. `src/jobs/streaming_job.py`

**Rôle :** Job de streaming structuré Kafka → Delta. Ingère des messages JSON
en temps réel et les écrit dans la zone Bronze.

### Classe `OrdersStreamingJob`

```python
job = OrdersStreamingJob(
    config=AppConfig.from_env(),
    trigger_interval="30 seconds",  # ou "once", "availableNow"
)

# Démarrage bloquant (attend Ctrl+C)
job.start(
    kafka_bootstrap_servers="kafka-broker1:9092,kafka-broker2:9092",
    topic="orders-events",
    await_termination=True,
)

# Démarrage non-bloquant (usage programmatique, tests)
query = job.start(
    kafka_bootstrap_servers="localhost:9092",
    topic="orders-events",
    await_termination=False,
)
# … traiter des batches …
job.stop()
```

### Options du trigger

| Valeur | Comportement |
|---|---|
| `"30 seconds"` | Traitement toutes les 30 secondes |
| `"once"` | Traite tous les offsets disponibles, puis s'arrête |
| `"availableNow"` | Comme `"once"` mais en multi-batch (Spark 3.3+) |
| `"1 minute"` | Toutes les minutes |

### Checkpointing

Le checkpoint est écrit dans `<checkpoint_path>/orders_streaming`.
Il contient les offsets Kafka déjà traités, ce qui garantit :
- Pas de relecture des mêmes messages après redémarrage
- Reprise exactement là où le job s'était arrêté

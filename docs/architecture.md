# Architecture du projet

Ce document explique les décisions d'architecture, les patterns de design utilisés,
les dépendances entre modules et les flux de données de bout en bout.

---

## Table des matières

1. [Vue d'ensemble architecturale](#1-vue-densemble-architecturale)
2. [Architecture Médaillon](#2-architecture-médaillon)
3. [Patterns de design](#3-patterns-de-design)
4. [Graphe de dépendances des modules](#4-graphe-de-dépendances-des-modules)
5. [Cycle de vie d'un job batch](#5-cycle-de-vie-dun-job-batch)
6. [Cycle de vie du streaming](#6-cycle-de-vie-du-streaming)
7. [Stratégie de configuration](#7-stratégie-de-configuration)
8. [Stratégie de qualité des données](#8-stratégie-de-qualité-des-données)
9. [Stratégie de tests](#9-stratégie-de-tests)
10. [Décisions d'architecture (ADR)](#10-décisions-darchitecture-adr)

---

## 1. Vue d'ensemble architecturale

Le projet est organisé en **couches horizontales** avec des responsabilités claires et
des dépendances toujours descendantes (jamais circulaires) :

```
┌─────────────────────────────────────────────────────────┐
│  COUCHE JOBS          src/jobs/                         │
│  Orchestration du cycle de vie ETL et streaming         │
├─────────────────────────────────────────────────────────┤
│  COUCHE DOMAINE                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐ │
│  │transformations│  │   quality/   │  │   schemas/    │ │
│  │   (métier)   │  │  (contrats)  │  │  (structure)  │ │
│  └──────────────┘  └──────────────┘  └───────────────┘ │
├─────────────────────────────────────────────────────────┤
│  COUCHE I/O           src/io/                           │
│  Abstraction lecture/écriture (formats multiples)       │
├─────────────────────────────────────────────────────────┤
│  COUCHE UTILITAIRES   src/utils/                        │
│  Config, Logger, SparkSession — aucune dépendance métier│
└─────────────────────────────────────────────────────────┘
```

**Règle fondamentale :** une couche ne peut importer que depuis la couche en dessous
ou la même couche. Les couches supérieures ne remontent jamais vers les couches inférieures.

---

## 2. Architecture Médaillon

### Les zones du Data Lake

```
Source Systems
      │
      ▼
┌──────────────────────────────────────────────────────────────────┐
│  RAW ZONE                                                        │
│  • Données brutes, intactes, telles que reçues de la source      │
│  • Format natif (CSV, JSON, Parquet source)                      │
│  • Jamais modifiées, conservées à titre d'audit                  │
│  • Partitionnement par date d'ingestion                          │
└──────────────────────────────────────────────────────────────────┘
      │
      ▼ (Ingestion structurée)
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE ZONE                                                     │
│  • Données structurées avec schema explicite (StructType)        │
│  • Colonnes d'audit ajoutées : _ingestion_ts, _source_file       │
│  • Format : Delta Lake (permet le time travel et les upserts)    │
│  • Aucune transformation métier — fidèle à la source             │
└──────────────────────────────────────────────────────────────────┘
      │
      ▼ (ETL batch / streaming → Silver)
┌──────────────────────────────────────────────────────────────────┐
│  SILVER ZONE                                                     │
│  • Données nettoyées, typées, enrichies                          │
│  • Doublons supprimés, NULLs traités, casses normalisées         │
│  • Colonnes calculées ajoutées (total_amount, full_name…)        │
│  • Validation qualité données avant écriture                     │
│  • Idempotence garantie par Delta upsert sur clé métier          │
└──────────────────────────────────────────────────────────────────┘
      │
      ▼ (Agrégations / modèles Gold)
┌──────────────────────────────────────────────────────────────────┐
│  GOLD ZONE                                                       │
│  • Agrégats prêts pour la consommation analytique                │
│  • Modèles de données optimisés (tables larges, star schema)     │
│  • Source de vérité pour les dashboards BI et les APIs           │
│  • Résultats de qualité données (gold/dq_results)                │
└──────────────────────────────────────────────────────────────────┘
```

### Schémas par zone

Le fichier [src/schemas/schemas.py](../src/schemas/schemas.py) définit les `StructType`
pour chaque combinaison zone/dataset. Le registry `SCHEMA_REGISTRY` permet un accès
programmatique sans import direct :

```python
# Nommage : "<zone>.<dataset>"
SCHEMA_REGISTRY = {
    "raw.orders":        RAW_ORDERS_SCHEMA,
    "raw.customers":     RAW_CUSTOMERS_SCHEMA,
    "raw.products":      RAW_PRODUCTS_SCHEMA,
    "silver.orders":     SILVER_ORDERS_SCHEMA,
    "silver.customers":  SILVER_CUSTOMERS_SCHEMA,
    "gold.daily_sales":  GOLD_DAILY_SALES_SCHEMA,
    "gold.customer_360": GOLD_CUSTOMER_360_SCHEMA,
}
```

---

## 3. Patterns de design

### 3.1 Template Method — `BaseJob`

`BaseJob` définit le squelette de l'algorithme ETL. Les sous-classes ne font qu'implémenter
les étapes variables, sans toucher à l'orchestration.

```
BaseJob.execute()           ← algorithme fixé, non surchargeable
    │
    ├── pre_run()           ← hook surchargeable (optionnel)
    ├── run()               ← ABSTRACT — chaque job l'implémente
    ├── post_run()          ← hook surchargeable (optionnel)
    └── on_failure(exc)     ← hook surchargeable (alerting Slack, PD…)
```

**Bénéfice :** le démarrage de la SparkSession, la collecte des métriques, la gestion
des exceptions et l'arrêt sont garantis identiques pour tous les jobs — zéro duplication.

### 3.2 Strategy — Readers et Writers

Les classes `ParquetReader`, `DeltaReader`, `CsvReader`, etc. implémentent toutes
l'interface `BaseReader.read()`. Le job ne connaît que l'interface, pas la
classe concrète. La factory `create_reader(spark, format)` fait la sélection.

```
                    ┌───────────────┐
                    │  BaseReader   │
                    │  .read()      │
                    └───────┬───────┘
           ┌────────────────┼───────────────────┐
           │                │                   │
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ ParquetReader│  │  DeltaReader │  │  CsvReader   │
    └──────────────┘  └──────────────┘  └──────────────┘
```

**Bénéfice :** changer le format source d'un job = changer une chaîne de caractères
dans la config, pas le code du job.

### 3.3 Composite + Chain of Responsibility — Transformers

Chaque `BaseTransformer` est un maillon. La fonction `compose()` les enchaîne en une
pipeline qui applique les transformations séquentiellement.

```python
# Chaque transformer est indépendant et testable en isolation
pipeline = compose(
    TrimStrings(),          # étape 1
    DropDuplicates(...),    # étape 2
    CastColumns(...),       # étape 3
    AddAuditColumns(...),   # étape 4
)
result = pipeline(df)   # exécution séquentielle
```

**Bénéfice :** les pipelines sont lisibles comme une recette. Ajouter/retirer une étape
= ajouter/retirer une ligne. Chaque étape est testable unitairement.

### 3.4 Factory Method — `create_reader` et `create_writer`

```python
def create_reader(spark: SparkSession, format: str) -> BaseReader:
    registry = {
        "parquet": ParquetReader,
        "delta":   DeltaReader,
        "csv":     CsvReader,
        "json":    JsonReader,
        "jdbc":    JdbcReader,
    }
    return registry[format.lower()](spark)
```

**Bénéfice :** extension sans modification (principe Open/Closed). Ajouter un format =
ajouter une entrée dans le registre.

### 3.5 Registry — Schemas

```python
schema = get_schema("silver.orders")  # lookup O(1) dans le dict
```

**Bénéfice :** un seul endroit pour tous les schémas. Toute modification est
visible en git et impacte tous les jobs qui utilisent ce schéma.

---

## 4. Graphe de dépendances des modules

Les flèches indiquent « importe depuis ». Les dépendances sont strictement descendantes.

```
src/jobs/etl_job.py
    │
    ├──► src/jobs/base_job.py
    │        └──► src/utils/config.py
    │        └──► src/utils/logger.py
    │        └──► src/utils/spark_utils.py
    │
    ├──► src/transformations/base_transformer.py
    │        └──► src/utils/logger.py
    │
    ├──► src/transformations/data_cleaner.py
    │        └──► src/transformations/base_transformer.py
    │        └──► src/utils/logger.py
    │
    ├──► src/io/readers.py
    │        └──► src/utils/logger.py
    │
    ├──► src/io/writers.py
    │        └──► src/utils/logger.py
    │
    ├──► src/quality/data_quality.py
    │        └──► src/utils/logger.py
    │
    └──► src/schemas/schemas.py   (aucune dépendance interne)
```

**Observation clé :** `src/utils/` est la seule couche sans dépendance interne.
Tous les autres modules peuvent l'importer. Elle constitue le socle du framework.

---

## 5. Cycle de vie d'un job batch

### Séquence complète

```
Appelant (CI, orchestrateur, main.py)
    │
    │  job.execute()
    ▼
BaseJob.execute()
    │
    │  [1] log "Job starting" avec env + job_name
    │  [2] metrics.start_time = time.monotonic()
    │
    ├──► pre_run()          (override optionnel : préparer des états, valider des paths)
    │
    ├──► run()              ◄── SEULE méthode à implémenter dans OrdersEtlJob
    │       │
    │       ├── extract()
    │       │      └── create_reader(spark, "parquet").read(source_path, schema=RAW_ORDERS_SCHEMA)
    │       │          metrics.rows_read = df.count()
    │       │
    │       ├── transform(raw_df)
    │       │      └── compose(TrimStrings, DropDuplicates, DropNullKeys, CastColumns,
    │       │                  ComputeDerivedColumns, AddAuditColumns)(df)
    │       │          metrics.rows_written = result.count()
    │       │          metrics.rows_dropped = rows_read - rows_written
    │       │
    │       ├── validate(silver_df)
    │       │      └── DataQualityChecker.run(df)
    │       │          → persist_results() si config.data_quality.enabled
    │       │          → assert_no_failures() si config.data_quality.fail_on_error
    │       │          metrics.dq_checks_passed / dq_checks_failed
    │       │
    │       └── load(silver_df)
    │              └── DeltaWriter.upsert(df, target_path, merge_keys=["order_id"])
    │
    ├──► [succès] post_run()   (override optionnel : notifier Slack, archiver)
    │    [échec]  on_failure() (override optionnel : alerter PagerDuty, Slack)
    │
    └──► [finally] stop_spark_session() si _owns_spark == True
         return metrics
```

### Gestion de la SparkSession

`BaseJob` utilise la propriété **lazy** `spark` :
- Si une `SparkSession` est injectée au constructeur → elle est utilisée et **non arrêtée**
  en fin de job (utile pour les tests et le mode Databricks)
- Si aucune session n'est fournie → elle est créée à la première utilisation et
  **arrêtée** dans le bloc `finally` de `execute()`

```python
# En test : injection de session partagée → pas de recreating, pas de teardown
job = OrdersEtlJob(spark=spark_fixture)

# En production : session créée et gérée par le job
job = OrdersEtlJob()   # spark créé lazily, arrêté après execute()
```

---

## 6. Cycle de vie du streaming

### Architecture du job streaming

```
Kafka Cluster
    │  (messages JSON, format RAW_ORDERS_SCHEMA)
    ▼
OrdersStreamingJob._build_source()
    │  readStream.format("kafka")
    │  option("maxOffsetsPerTrigger", 100_000)   ← contrôle de la vitesse
    ▼
_parse_messages()
    │  from_json(value, RAW_ORDERS_SCHEMA)        ← désérialisation sécurisée
    │  + extraction offset, partition, kafka_ts
    ▼
writeStream.foreachBatch(_write_batch)
    │  trigger(processingTime="30 seconds")       ← micro-batch configurable
    │  checkpointLocation = <checkpoint_path>/orders_streaming
    ▼
_write_batch(batch_df, batch_id)
    │
    ├── _transform_batch()   TrimStrings + AddAuditColumns
    │
    └── write.format("delta").mode("append")
              → Bronze Delta Table
```

### Garanties de fiabilité

| Garantie | Mécanisme |
|---|---|
| At-least-once delivery | Checkpointing Spark Structured Streaming |
| Pas de perte de données au redémarrage | `checkpointLocation` persiste les offsets Kafka |
| Idempotence au niveau batch | Delta Lake gère les ré-exécutions via `foreachBatch` |
| Contrôle du débit | `maxOffsetsPerTrigger` limite les offsets par micro-batch |
| Tolérance à la perte de données Kafka | `failOnDataLoss=false` |

---

## 7. Stratégie de configuration

### Principe des deux couches

```
┌──────────────────────────────────────────────────────────┐
│  Couche 2 : Variables d'environnement (priorité haute)   │
│  SPARK_APP_NAME, DATA_LAKE_PATH, LOG_LEVEL, …            │
│  → Idéal pour les secrets, les surcharges CI/CD          │
└────────────────────────┬─────────────────────────────────┘
                         │ os.getenv() avec fallback
┌────────────────────────▼─────────────────────────────────┐
│  Couche 1 : configs/<env>.yaml (priorité basse)          │
│  spark.app_name, storage.data_lake_path, …               │
│  → Valeurs stables, versionnées en git                   │
└──────────────────────────────────────────────────────────┘
```

### Résolution de l'environnement

```python
AppConfig.from_env()
    │
    ├── lit $ENV (ou défaut "dev")
    ├── charge configs/<env>.yaml
    └── applique les overrides env vars par-dessus
```

---

## 8. Stratégie de qualité des données

### Architecture du moteur DQ

```
DataQualityChecker("silver.orders")
    │
    │  .add_check(check)  ← retourne self → chaînage fluide
    │
    ▼
List[BaseCheck]
    │
    │  .run(df)
    ▼
Pour chaque check:
    check.run(df, dataset_name)
        │
        ├── Compte les lignes totales et défaillantes
        ├── Calcule le pass_rate
        └── Retourne un CheckResult(status, severity, metrics, details)

    │  [si passed] logger.info
    │  [si failed] logger.warning
    ▼
List[CheckResult]
    │
    ├── .persist_results(spark, path)  → Delta Table gold/dq_results
    │
    └── .assert_no_failures()
            │
            └── [si FAIL severity non passé] raise DataQualityError
```

### Matrice sévérité × comportement

| Sévérité | Check échoue | Comportement | Cas d'usage |
|---|---|---|---|
| `WARN` | `status = "WARN"` | Log warning, pipeline continue | Valeurs inattendues non bloquantes |
| `FAIL` | `status = "FAIL"` | Log error, `assert_no_failures()` lève | Clés nulles, doublons sur PK |

### Persistance des résultats

Chaque run DQ écrit dans `gold/dq_results` un enregistrement par check :

```
check_name    │ dataset        │ status │ pass_rate │ failing_rows │ run_ts
──────────────┼────────────────┼────────┼───────────┼──────────────┼──────────────
not_null:id   │ silver.orders  │ PASS   │ 1.0       │ 0            │ 2024-01-15T…
unique:id     │ silver.orders  │ PASS   │ 1.0       │ 0            │ 2024-01-15T…
accepted:stat │ silver.orders  │ WARN   │ 0.998     │ 42           │ 2024-01-15T…
```

Ces données alimentent les dashboards de monitoring qualité.

---

## 9. Stratégie de tests

### Pyramide de tests

```
          ┌─────────────────┐
          │  Integration    │  Lents (~30s) — SparkSession local
          │  tests/         │  Testent le pipeline complet
          │  integration/   │  (extract + transform + validate)
          ├─────────────────┤
          │   Unit tests    │  Rapides (~5s) — DataFrame in-memory
          │   tests/unit/   │  Testent chaque transformer, check DQ,
          │                 │  config en isolation
          └─────────────────┘
```

### Principe d'isolation

Les tests unitaires instancient chaque transformer directement et lui passent
un DataFrame créé en mémoire via `spark.createDataFrame()`. Aucune lecture ni
écriture de fichier — les tests sont donc exécutables sans réseau ni stockage.

```python
def test_drop_duplicates(spark):
    data = [("A", 1), ("A", 1), ("B", 2)]
    df = spark.createDataFrame(data, ["id", "val"])
    result = DropDuplicates()(df)
    assert result.count() == 2
```

### Session Spark partagée

`conftest.py` expose une `SparkSession` avec scope `session` pour éviter de
recréer un contexte Spark à chaque test (coût ~5 secondes au démarrage) :

```python
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return get_spark_session(testing=True)   # local[1], shuffle_partitions=1
```

---

## 10. Décisions d'architecture (ADR)

### ADR-001 : Delta Lake comme format de stockage principal

**Décision :** Toutes les zones Bronze, Silver et Gold utilisent Delta Lake.

**Raisons :**
- ACID transactions → pas de lectures partielles lors des écritures
- Time travel → reproductibilité des pipelines, débogage facilité
- Schema evolution → `mergeSchema` pour les ajouts de colonnes non-breaking
- Upsert/merge natif → idempotence sans réécriture complète de partition
- Compatible avec Databricks, AWS EMR, Azure HDInsight et open source

### ADR-002 : Transformers stateless sans dépendances Spark injectées

**Décision :** Les transformers reçoivent un `DataFrame` et retournent un `DataFrame`.
Ils ne stockent pas de SparkSession ni de config.

**Raisons :**
- Testabilité maximale : un test = `transformer(df)`, pas de mock complexe
- Réutilisabilité : un transformer peut être utilisé dans n'importe quel job
- Composabilité : `compose()` fonctionne sur tout callable `df → df`

### ADR-003 : Schemas centralisés dans `schemas.py`

**Décision :** Aucun `StructType` n'est défini directement dans les jobs ou transformers.

**Raisons :**
- Un seul endroit pour gérer l'évolution des schémas
- Les changements de schema sont visibles en code review (pas cachés dans un job)
- Le registry permet de valider à l'ingestion sans dépendance circulaire

### ADR-004 : Configuration YAML + override env vars

**Décision :** Valeurs par défaut en YAML, secrets et surcharges par variables d'environnement.

**Raisons :**
- Les YAMLs sont versionnés en git → traçabilité des changements de config
- Les secrets (tokens, mots de passe) ne sont jamais dans git
- Compatible avec tous les systèmes de secrets management (Vault, AWS Secrets Manager, K8s secrets)

### ADR-005 : Qualité des données avant écriture, pas après lecture

**Décision :** Les checks DQ sont exécutés sur le DataFrame transformé, juste avant l'écriture en Silver/Gold.

**Raisons :**
- Valide la donnée dans l'état où elle sera persistée (après transformation)
- Bloque l'écriture de données corrompues en Silver/Gold
- Les résultats sont persistés en Gold pour le monitoring longitudinal

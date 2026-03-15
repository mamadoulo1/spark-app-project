# =============================================================================
# ETAPE 9 — Delta Lake + Readers / Writers
#
# On apprend :
#   - Pourquoi mode("overwrite") est dangereux et mode("append") cree des doublons
#   - L'idempotence : rejouer un job 2 fois = meme resultat
#   - Delta MERGE / upsert : la solution professionnelle
#   - src/io/readers.py : factory de readers (Parquet, CSV, JSON, Delta)
#   - src/io/writers.py : factory de writers avec DeltaWriter.upsert()
# =============================================================================

import os
import sys

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"]        = "127.0.0.1"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["LOG_FORMAT"]            = "text"
os.environ["LOG_LEVEL"]             = "INFO"

from src.utils.logger import get_logger
from src.utils.config import AppConfig
from src.utils.spark_utils import get_spark_session
from src.jobs.base_job import BaseJob
from src.transformations.base_transformer import compose
from src.transformations.data_cleaner import (
    TrimStrings, DropDuplicates, DropNullKeys,
    CastColumns, ComputeDerivedColumns, AddAuditColumns,
)
from src.quality.data_quality import (
    DataQualityChecker,
    NotNullCheck, UniquenessCheck, AcceptedValuesCheck,
    RowCountCheck, RangeCheck,
)
from src.io.readers import create_reader
from src.io.writers import create_writer, DeltaWriter

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)
logger = get_logger(__name__)
config = AppConfig.from_env()
spark  = get_spark_session(config)

RAW_ORDERS_SCHEMA = StructType([
    StructField("order_id",    StringType(),       nullable=False),
    StructField("customer_id", StringType(),       nullable=False),
    StructField("product_id",  StringType(),       nullable=True),
    StructField("quantity",    IntegerType(),      nullable=True),
    StructField("unit_price",  DecimalType(18, 4), nullable=True),
    StructField("status",      StringType(),       nullable=True),
    StructField("order_date",  DateType(),         nullable=True),
    StructField("channel",     StringType(),       nullable=True),
])

VALID_STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
DELTA_PATH     = "data/lake/silver/orders_delta"


# =============================================================================
# PARTIE A — Le probleme de l'idempotence
# =============================================================================

print("\n========== PARTIE A : LE PROBLEME DE L'IDEMPOTENCE ==========\n")

print("Scenario : l'orchestrateur (Airflow, ADF) rejoue un job qui a timeout")
print()
print("mode('overwrite')")
print("  Run 1 : 1000 lignes ecrites  -> OK")
print("  Run 2 : 1000 lignes ecrites  -> OK, mais si la source a change entre-temps")
print("          les anciennes donnees d'autres partitions sont PERDUES")
print()
print("mode('append')")
print("  Run 1 : 1000 lignes inserees -> OK")
print("  Run 2 : 1000 lignes inserees -> 2000 lignes ! DOUBLONS garantis")
print()
print("Delta upsert (MERGE)")
print("  Run 1 : 1000 lignes inserees -> OK")
print("  Run 2 : 1000 lignes : UPDATE si existant, INSERT si nouveau -> toujours 1000")
print("  => Idempotent : N executions = meme resultat")


# =============================================================================
# PARTIE B — Demonstration du probleme avec mode("append")
# =============================================================================

print("\n========== PARTIE B : DEMO mode('append') = doublons ==========\n")

import shutil, os as _os
if _os.path.exists("data/lake/silver/orders_append"):
    shutil.rmtree("data/lake/silver/orders_append")

df_initial = spark.createDataFrame([
    ("ORD-001", "CUST-1", "PROD-A", 2, "PENDING"),
    ("ORD-002", "CUST-2", "PROD-B", 1, "SHIPPED"),
], ["order_id", "customer_id", "product_id", "quantity", "status"])

# Premier run
df_initial.write.mode("append").parquet("data/lake/silver/orders_append")
count_1 = spark.read.parquet("data/lake/silver/orders_append").count()
print(f"Apres run 1 (append) : {count_1} lignes")

# Deuxieme run (simule un rejeu)
df_initial.write.mode("append").parquet("data/lake/silver/orders_append")
count_2 = spark.read.parquet("data/lake/silver/orders_append").count()
print(f"Apres run 2 (append) : {count_2} lignes  <- DOUBLONS !")
assert count_2 == 2 * count_1, "Doublons confirmes"
print("=> En production : aggregations fausses, KPIs incorrects")


# =============================================================================
# PARTIE C — La solution : Delta upsert
# =============================================================================

print("\n========== PARTIE C : DELTA UPSERT (idempotent) ==========\n")

if _os.path.exists(DELTA_PATH):
    shutil.rmtree(DELTA_PATH)

writer = DeltaWriter(spark)

# Donnees initiales
df_jour1 = spark.createDataFrame([
    ("ORD-001", "CUST-1", "PROD-A", 2, "PENDING"),
    ("ORD-002", "CUST-2", "PROD-B", 1, "SHIPPED"),
], ["order_id", "customer_id", "product_id", "quantity", "status"])

# Run 1 : premiere ecriture (cree la table Delta)
writer.upsert(df_jour1, DELTA_PATH, merge_keys=["order_id"])
count_r1 = spark.read.format("delta").load(DELTA_PATH).count()
print(f"Apres run 1 : {count_r1} lignes")

# Run 2 : memes donnees (simule un rejeu de l'orchestrateur)
writer.upsert(df_jour1, DELTA_PATH, merge_keys=["order_id"])
count_r2 = spark.read.format("delta").load(DELTA_PATH).count()
print(f"Apres run 2 (rejeu) : {count_r2} lignes  <- TOUJOURS {count_r2}, pas de doublons")
assert count_r1 == count_r2, "Idempotence verifiee"

# Run 3 : nouvelles donnees + mise a jour d'une commande existante
df_jour2 = spark.createDataFrame([
    ("ORD-001", "CUST-1", "PROD-A", 2, "DELIVERED"),  # mise a jour du statut
    ("ORD-003", "CUST-3", "PROD-C", 5, "PENDING"),    # nouvelle commande
], ["order_id", "customer_id", "product_id", "quantity", "status"])

writer.upsert(df_jour2, DELTA_PATH, merge_keys=["order_id"])
df_final = spark.read.format("delta").load(DELTA_PATH)
count_r3 = df_final.count()
print(f"\nApres run 3 (nouvelles donnees) : {count_r3} lignes")
print("Contenu de la table Silver :")
df_final.orderBy("order_id").show(truncate=False)
print("ORD-001 -> statut mis a jour : PENDING -> DELIVERED")
print("ORD-002 -> inchange")
print("ORD-003 -> insere")


# =============================================================================
# PARTIE D — Time Travel Delta
# =============================================================================

print("\n========== PARTIE D : TIME TRAVEL DELTA ==========\n")

from src.io.readers import DeltaReader

reader = DeltaReader(spark)

# Version 0 : apres le run 1 (2 lignes, ORD-001 en PENDING)
df_v0 = reader.read(DELTA_PATH, version=0)
print(f"Version 0 (apres run 1) : {df_v0.count()} lignes")
df_v0.show(truncate=False)

# Version courante : 3 lignes, ORD-001 en DELIVERED
df_current = reader.read(DELTA_PATH)
print(f"Version courante : {df_current.count()} lignes")
df_current.orderBy("order_id").show(truncate=False)

print("=> Le time travel permet de reproduire exactement l'etat d'une table")
print("   a n'importe quelle version passee")


# =============================================================================
# PARTIE E — Factory readers/writers
# =============================================================================

print("\n========== PARTIE E : FACTORY (create_reader / create_writer) ==========\n")

# Le job connait uniquement le format, pas la classe concrete
reader_csv   = create_reader(spark, "csv")
reader_delta = create_reader(spark, "delta")
writer_delta = create_writer(spark, "delta")

print(f"create_reader(spark, 'csv')   -> {type(reader_csv).__name__}")
print(f"create_reader(spark, 'delta') -> {type(reader_delta).__name__}")
print(f"create_writer(spark, 'delta') -> {type(writer_delta).__name__}")
print()
print("=> Pour changer le format source : changer une chaine de caracteres")
print("   Pas de modification du code du job")


# =============================================================================
# PARTIE F — OrdersEtlJob : version finale avec readers/writers
# =============================================================================

print("\n========== PARTIE F : JOB FINAL AVEC READERS/WRITERS ==========\n")

class OrdersEtlJob(BaseJob):
    """
    Version finale du job ETL Orders.
    Utilise les readers/writers + DQ + transformers.
    """
    JOB_NAME = "orders_etl"

    def __init__(self, config=None, spark=None):
        super().__init__(config=config, spark=spark)
        self.source_path = "data/orders.csv"
        self.target_path = self.config.get_zone_path("silver", "orders_v2")
        self.dq_path     = self.config.get_zone_path("gold", "dq_results")

    def _extract(self) -> DataFrame:
        df = create_reader(self.spark, "csv").read(
            self.source_path,
            schema=RAW_ORDERS_SCHEMA,
        )
        self.metrics.rows_read = df.count()
        logger.info("Extraction OK", extra={"rows": self.metrics.rows_read})
        return df

    def _transform(self, df: DataFrame) -> DataFrame:
        result = compose(
            TrimStrings(),
            DropDuplicates(subset=["order_id"]),
            DropNullKeys(key_columns=["order_id", "customer_id"]),
            CastColumns({"quantity": "integer", "unit_price": "decimal(18,4)"}),
            ComputeDerivedColumns({
                "total_amount": "ROUND(CAST(quantity AS DECIMAL(18,4)) * unit_price, 2)",
                "status":       "UPPER(COALESCE(status, 'UNKNOWN'))",
            }),
            AddAuditColumns(pipeline_name=self.JOB_NAME, env=self.config.env),
        )(df)
        self.metrics.rows_written = result.count()
        self.metrics.rows_dropped = self.metrics.rows_read - self.metrics.rows_written
        return result

    def _validate(self, df: DataFrame) -> None:
        checker = (
            DataQualityChecker(f"{self.JOB_NAME}.silver_orders")
            .add_check(RowCountCheck(min_rows=1))
            .add_check(NotNullCheck("order_id"))
            .add_check(NotNullCheck("customer_id"))
            .add_check(UniquenessCheck("order_id"))
            .add_check(RangeCheck("unit_price", min_val=0.0))
            .add_check(AcceptedValuesCheck("status", VALID_STATUSES))
        )
        results = checker.run(df)
        self.metrics.dq_checks_passed = sum(1 for r in results if r.passed)
        self.metrics.dq_checks_failed = len(results) - self.metrics.dq_checks_passed
        if self.config.data_quality.fail_on_error:
            checker.assert_no_failures()

    def _load(self, df: DataFrame) -> None:
        # upsert Delta : idempotent sur la cle metier
        create_writer(self.spark, "delta").upsert(
            df=df,
            path=self.target_path,
            merge_keys=["order_id"],
        )
        logger.info("Chargement OK", extra={"target": self.target_path})

    def run(self) -> None:
        df_raw    = self._extract()
        df_silver = self._transform(df_raw)
        self._validate(df_silver)
        self._load(df_silver)

# Executer 2 fois pour prouver l'idempotence
print("Execution 1 :")
job1 = OrdersEtlJob(spark=spark)
m1   = job1.execute()
count1 = spark.read.format("delta").load(job1.target_path).count()
print(f"  Lignes dans la table Silver : {count1}")

print("\nExecution 2 (rejeu, memes donnees) :")
job2 = OrdersEtlJob(spark=spark)
m2   = job2.execute()
count2 = spark.read.format("delta").load(job2.target_path).count()
print(f"  Lignes dans la table Silver : {count2}")
assert count1 == count2, "Idempotence verifiee"
print(f"\n  Run 1 = {count1} lignes, Run 2 = {count2} lignes")
print("  => Idempotent : rejouer le job ne cree pas de doublons")

spark.stop()

print("\n========== RESUME ETAPE 9 ==========\n")
print("Modules crees :")
print("  src/io/readers.py : ParquetReader, CsvReader, JsonReader, DeltaReader")
print("  src/io/writers.py : ParquetWriter, DeltaWriter (avec upsert())")
print()
print("Concepts cles :")
print("  Idempotence   : N executions avec les memes donnees = meme resultat")
print("  Delta MERGE   : UPDATE si existe, INSERT si nouveau, ACID garanti")
print("  Time Travel   : relire n'importe quelle version passee de la table")
print("  Factory       : create_reader/writer -> le job ne connait que le format")

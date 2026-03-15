# =============================================================================
# run_pipeline.py
#
# ETAPE 11 — Orchestration de pipeline
#
# On apprend :
#   - Pipeline DAG : steps avec dependances (comme Airflow / ADF / Prefect)
#   - Fail-fast     : si un step echoue, les suivants passent en SKIPPED
#   - Retry         : un step peut etre rejoue automatiquement
#   - Idempotence   : rejouer le pipeline entier = meme resultat (Delta MERGE)
#   - PipelineReport : rapport d'execution lisible par les ops
#
# Structure du pipeline de ce projet :
#
#   [bronze_ingest]  →  [silver_orders]  →  [gold_kpis]  →  [dq_report]
#
#   Chaque fleche = dependance :
#     silver_orders ne tourne pas si bronze_ingest a echoue
#     gold_kpis     ne tourne pas si silver_orders a echoue
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
from src.pipeline.pipeline import Pipeline, PipelineStep, StepStatus
from src.jobs.etl_job import OrdersEtlJob
from src.jobs.base_job import BaseJob, JobMetrics

from pyspark.sql import DataFrame

logger = get_logger(__name__)
config = AppConfig.from_env()
spark  = get_spark_session(config)


# =============================================================================
# JOBS SUPPLEMENTAIRES (simules pour la demo)
# On cree des jobs simples qui illustrent chaque zone du Medallion
# =============================================================================

class BronzeIngestJob(BaseJob):
    """
    Simule l'ingestion de la zone Raw vers Bronze.
    En production : lecture depuis S3/ADLS, ecriture Parquet partitionnee.
    """
    JOB_NAME = "bronze_ingest"

    def run(self) -> None:
        logger.info("Ingestion Bronze : lecture des sources Raw")
        # Simulation : on lit le CSV source et on le copie en Parquet Bronze
        df = spark.read.option("header", "true").csv("data/orders.csv")
        bronze_path = self.config.get_zone_path("bronze", "orders_raw")
        df.write.mode("overwrite").parquet(bronze_path)
        self.metrics.rows_read    = df.count()
        self.metrics.rows_written = self.metrics.rows_read
        logger.info(
            "Ingestion Bronze terminee",
            extra={"rows": self.metrics.rows_written, "path": bronze_path}
        )


class GoldKpisJob(BaseJob):
    """
    Calcule les KPIs metier depuis la zone Silver.
    En production : aggreagations complexes, ecriture Gold partitionnee.
    """
    JOB_NAME = "gold_kpis"

    def run(self) -> None:
        silver_path = self.config.get_zone_path("silver", "orders_v2")
        logger.info("Calcul Gold KPIs", extra={"source": silver_path})

        df_silver = spark.read.format("delta").load(silver_path)

        # KPI 1 : Chiffre d'affaires par statut
        df_kpis = df_silver.groupBy("status").agg(
            {"total_amount": "sum", "order_id": "count"}
        )

        gold_path = self.config.get_zone_path("gold", "orders_kpis")
        df_kpis.write.mode("overwrite").parquet(gold_path)

        self.metrics.rows_read    = df_silver.count()
        self.metrics.rows_written = df_kpis.count()
        logger.info(
            "Gold KPIs calcules",
            extra={"kpi_rows": self.metrics.rows_written, "path": gold_path}
        )


class DqReportJob(BaseJob):
    """
    Genere un rapport de qualite des donnees au format Parquet.
    Ce job est utile pour les tableaux de bord de monitoring.
    """
    JOB_NAME = "dq_report"

    def run(self) -> None:
        silver_path = self.config.get_zone_path("silver", "orders_v2")
        logger.info("Generation rapport DQ", extra={"source": silver_path})

        df = spark.read.format("delta").load(silver_path)

        # Rapport DQ simple : compte des valeurs par statut
        df_report = df.groupBy("status").count().withColumnRenamed("count", "nb_orders")
        dq_path = self.config.get_zone_path("gold", "dq_summary")
        df_report.write.mode("overwrite").parquet(dq_path)

        self.metrics.rows_read    = df.count()
        self.metrics.rows_written = df_report.count()
        logger.info("Rapport DQ genere", extra={"path": dq_path})


# =============================================================================
# PARTIE A — Pipeline nominal (tous les steps reussissent)
# =============================================================================

print("\n========== PARTIE A : PIPELINE NOMINAL ==========\n")
print("DAG du pipeline :")
print("  [bronze_ingest] -> [silver_orders] -> [gold_kpis] -> [dq_report]")
print()

pipeline = (
    Pipeline("orders_daily", spark=spark)
    .add_step(PipelineStep(
        name="bronze_ingest",
        job_class=BronzeIngestJob,
        config=config,
    ))
    .add_step(PipelineStep(
        name="silver_orders",
        job_class=OrdersEtlJob,
        dependencies=["bronze_ingest"],
        config=config,
    ))
    .add_step(PipelineStep(
        name="gold_kpis",
        job_class=GoldKpisJob,
        dependencies=["silver_orders"],
        config=config,
    ))
    .add_step(PipelineStep(
        name="dq_report",
        job_class=DqReportJob,
        dependencies=["silver_orders"],
        config=config,
    ))
)

report_a = pipeline.run()
report_a.print_summary()

assert report_a.success, "Le pipeline nominal doit reussir"
assert len(report_a.steps_failed)  == 0
assert len(report_a.steps_skipped) == 0
assert len(report_a.steps_success) == 4
print("=> Verification : 4 steps SUCCESS, 0 FAILED, 0 SKIPPED")


# =============================================================================
# PARTIE B — Idempotence du pipeline
# On rejoue exactement le meme pipeline
# =============================================================================

print("\n========== PARTIE B : IDEMPOTENCE (rejeu du pipeline) ==========\n")
print("Meme pipeline, deuxieme execution (simule un rejeu orchestrateur)")
print()

report_b = pipeline.run()
report_b.print_summary()

assert report_b.success
print("=> Idempotent : rejouer le pipeline ne cree pas de doublons")
print("   (Delta MERGE garantit l'unicite sur order_id)")


# =============================================================================
# PARTIE C — Echec d'un step et propagation SKIPPED
# =============================================================================

print("\n========== PARTIE C : ECHEC + PROPAGATION SKIPPED ==========\n")
print("Scenario : bronze_ingest echoue -> silver et gold passes en SKIPPED")
print()


class BrokenBronzeJob(BaseJob):
    """Simule un job qui echoue (source indisponible, reseau, etc.)."""
    JOB_NAME = "bronze_ingest"

    def run(self) -> None:
        raise RuntimeError("Source S3 indisponible : connection refused")


pipeline_with_failure = (
    Pipeline("orders_daily_broken", spark=spark)
    .add_step(PipelineStep(
        name="bronze_ingest",
        job_class=BrokenBronzeJob,
        config=config,
    ))
    .add_step(PipelineStep(
        name="silver_orders",
        job_class=OrdersEtlJob,
        dependencies=["bronze_ingest"],
        config=config,
    ))
    .add_step(PipelineStep(
        name="gold_kpis",
        job_class=GoldKpisJob,
        dependencies=["silver_orders"],
        config=config,
    ))
)

report_c = pipeline_with_failure.run()
report_c.print_summary()

assert not report_c.success
assert len(report_c.steps_failed)  == 1
assert len(report_c.steps_skipped) == 2
assert report_c.steps_failed[0].step_name  == "bronze_ingest"
assert report_c.steps_skipped[0].step_name == "silver_orders"
assert report_c.steps_skipped[1].step_name == "gold_kpis"
print("=> Verification : 1 FAILED (bronze), 2 SKIPPED (silver + gold)")
print("   Les donnees Silver et Gold sont intactes (pas de corruption partielle)")


# =============================================================================
# PARTIE D — Retry automatique
# =============================================================================

print("\n========== PARTIE D : RETRY AUTOMATIQUE ==========\n")
print("Un step avec retry_on_fail=True est rejoue automatiquement")
print()

_attempt_counter = {"n": 0}

class FlakyJob(BaseJob):
    """Simule un job instable qui echoue a la premiere tentative."""
    JOB_NAME = "flaky_job"

    def run(self) -> None:
        _attempt_counter["n"] += 1
        if _attempt_counter["n"] == 1:
            raise RuntimeError("Erreur transitoire (reseau, quota, etc.)")
        # Deuxieme tentative : succes
        self.metrics.rows_written = 42
        logger.info("FlakyJob : deuxieme tentative reussie")


pipeline_retry = (
    Pipeline("retry_demo", spark=spark)
    .add_step(PipelineStep(
        name="flaky_step",
        job_class=FlakyJob,
        config=config,
        retry_on_fail=True,   # <- active le retry
    ))
)

report_d = pipeline_retry.run()
report_d.print_summary()

assert report_d.success
assert _attempt_counter["n"] == 2   # 2 tentatives
print(f"=> Verification : {_attempt_counter['n']} tentatives, step SUCCESS")
print("   En production : utile pour les erreurs transitoires (S3 throttling, etc.)")


# =============================================================================
# FIN
# =============================================================================

spark.stop()

print("\n========== RESUME ETAPE 11 ==========\n")
print("Module cree :")
print("  src/pipeline/pipeline.py : Pipeline, PipelineStep, PipelineReport")
print()
print("Concepts cles :")
print("  DAG          : steps avec dependances, execution dans l'ordre")
print("  Fail-fast    : echec -> steps suivants en SKIPPED (pas de corruption)")
print("  Idempotence  : rejouer le pipeline = meme resultat (Delta MERGE)")
print("  Retry        : retry_on_fail=True pour les erreurs transitoires")
print("  PipelineReport : rapport lisible pour le monitoring ops")
print()
print("Analogie Airflow :")
print("  Pipeline     = DAG")
print("  PipelineStep = Operator / Task")
print("  dependencies = upstream_tasks")
print("  StepStatus   = TaskInstance state")

# =============================================================================
# src/jobs/etl_job.py
#
# Job ETL Orders — version finale avec readers/writers.
#
# Ce fichier contient la logique metier du pipeline Orders.
# L'infrastructure (Spark, logging, metriques, cycle de vie) est dans BaseJob.
#
# Pipeline : Raw CSV -> Silver Delta (idempotent via MERGE)
#   Extract   : lecture CSV avec schema explicite
#   Transform : nettoyage, dedup, calcul total_amount, colonnes d'audit
#   Validate  : 6 checks DQ (not null, unique, range, accepted values)
#   Load      : upsert Delta sur order_id (idempotent)
# =============================================================================

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from data_io.readers import create_reader
from data_io.writers import DeltaWriter
from jobs.base_job import BaseJob
from quality.data_quality import (
    AcceptedValuesCheck,
    DataQualityChecker,
    NotNullCheck,
    RangeCheck,
    RowCountCheck,
    UniquenessCheck,
)
from schemas.schemas import RAW_ORDERS_SCHEMA
from transformations.base_transformer import compose
from transformations.data_cleaner import (
    AddAuditColumns,
    CastColumns,
    ComputeDerivedColumns,
    DropDuplicates,
    DropNullKeys,
    TrimStrings,
)
from utils.config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__)

VALID_STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]


class OrdersEtlJob(BaseJob):
    """
    Pipeline ETL pour les commandes (orders).

    Lit depuis un CSV source, applique les transformations Silver,
    valide la qualite des donnees, et ecrit en Delta avec upsert idempotent.

    Usage :
        job = OrdersEtlJob()
        metrics = job.execute()

    Pour les tests : injecter spark et config pour eviter la creation
    d'une nouvelle SparkSession :
        job = OrdersEtlJob(config=config, spark=spark)
        result = job.transform(df)
    """

    JOB_NAME = "orders_etl"

    def __init__(
        self,
        config: AppConfig | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        super().__init__(config=config, spark=spark)
        # RAW_DATA_PATH : variable d'env injectée par Databricks (spark_env_vars).
        # Fallback local : data/orders.csv pour les tests et le dev local.
        import os

        self.source_path = os.getenv(
            "RAW_DATA_PATH",
            self.config.storage.data_lake_path.rstrip("/") + "/raw/orders.csv"
            if self.config.storage.data_lake_path
            else "data/orders.csv",
        )
        self.target_path = self.config.get_zone_path("silver", "orders_v2")
        self.dq_path = self.config.get_zone_path("gold", "dq_results")

    # ------------------------------------------------------------------
    # Methodes publiques — utilisees par les tests et les orchestrateurs
    # ------------------------------------------------------------------

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique les transformations Silver sur le DataFrame brut.

        Expose publiquement _transform() pour permettre aux tests de tester
        la logique de transformation en isolation, sans I/O.
        """
        return self._transform(df)

    def validate(self, df: DataFrame) -> None:
        """
        Execute les checks DQ sur le DataFrame transforme.

        Expose publiquement _validate() pour les tests.
        """
        self._validate(df)

    # ------------------------------------------------------------------
    # Implementation du Template Method (steps prives)
    # ------------------------------------------------------------------

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
            ComputeDerivedColumns(
                {
                    "total_amount": "ROUND(CAST(quantity AS DECIMAL(18,4)) * unit_price, 2)",
                    "status": "UPPER(COALESCE(status, 'UNKNOWN'))",
                }
            ),
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
        DeltaWriter(self.spark).upsert(
            df=df,
            path=self.target_path,
            merge_keys=["order_id"],
        )
        # Enregistre la table dans hive_metastore.default pour le SQL Editor.
        # Idempotent : IF NOT EXISTS ne recrée pas si elle existe déjà.
        # En local (tests) : crée une entrée dans le metastore Derby local, sans impact.
        # Enregistre dans hive_metastore (compatible DBFS + Unity Catalog activé).
        # Unity Catalog n'accepte pas dbfs:/ → on cible explicitement hive_metastore.default.
        # En local (tests) : Derby metastore, la clause hive_metastore. est ignorée.
        try:
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS hive_metastore.default.silver_orders "
                f"USING DELTA LOCATION '{self.target_path}'"
            )
        except Exception as e:
            # Fallback local sans préfixe catalogue (Derby ne supporte pas hive_metastore.)
            logger.warning("Enregistrement metastore hive_metastore échoué, tentative sans préfixe: %s", e)
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS default.silver_orders "
                f"USING DELTA LOCATION '{self.target_path}'"
            )
        logger.info("Chargement OK", extra={"target": self.target_path})

    def run(self) -> None:
        df_raw = self._extract()
        df_silver = self._transform(df_raw)
        self._validate(df_silver)
        self._load(df_silver)

# =============================================================================
# src/utils/spark_utils.py
#
# Pourquoi une factory SparkSession ?
#
#   Sans cette factory, chaque job ferait :
#     spark = SparkSession.builder.appName("...").master("...").getOrCreate()
#
#   Problemes :
#     - Duplication : chaque job reecrit la meme configuration
#     - Incohérence : un job oublie .config("spark.sql.shuffle.partitions", ...)
#     - Tests difficiles : impossible de detecter facilement si un job
#       cree sa propre session au lieu d'utiliser celle injectee
#
#   Solution : un seul endroit qui cree la SparkSession.
#   Tous les jobs appellent get_spark_session(config).
#
# Usage :
#   spark = get_spark_session(config)           # production / dev
#   spark = get_spark_session(testing=True)     # tests unitaires
# =============================================================================

from __future__ import annotations

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.utils.config import AppConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_spark_session(
    config: AppConfig | None = None,
    *,
    testing: bool = False,
) -> SparkSession:
    """
    Cree ou recupere une SparkSession configuree pour ce projet.

    Comportement :
      - Si une session active existe deja (getOrCreate), elle est reutilisee.
        C'est le comportement standard de Spark : une seule session par JVM.
      - En mode test (testing=True) : local[1], shuffle_partitions=1,
        pour des tests rapides et deterministes.

    Args:
        config  : Configuration de l'application. Si None, chargee depuis $ENV.
        testing : Si True, cree une session minimale pour les tests.

    Returns:
        SparkSession configuree et prete a l'emploi.
    """
    if config is None:
        config = AppConfig.from_env()

    if testing:
        # Session minimale pour les tests : local[1] = 1 seul thread
        logger.debug("Creation SparkSession en mode test")
        builder = (
            SparkSession.builder
            .appName("test-session")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.host",            "127.0.0.1")
            .config("spark.driver.bindAddress",     "127.0.0.1")
            .config("spark.ui.enabled",             "false")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    # Session de production / developpement
    logger.debug(
        "Creation SparkSession",
        extra={"app_name": config.spark.app_name, "master": config.spark.master}
    )

    builder = (
        SparkSession.builder
        .appName(config.spark.app_name)
        .master(config.spark.master)
        .config("spark.sql.shuffle.partitions",
                str(config.spark.shuffle_partitions))
        .config("spark.sql.adaptive.enabled",
                str(config.spark.adaptive_query_execution).lower())
        .config("spark.driver.host",        "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    for key, value in config.spark.extra_configs.items():
        builder = builder.config(key, value)

    # Active les extensions Delta Lake (MERGE, time travel, ACID transactions)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(config.spark.log_level)

    # Silencer le ShutdownHookManager (message d'erreur cosmétique sur Windows)
    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getLogger(
        "org.apache.spark.util.ShutdownHookManager"
    ).setLevel(log4j.Level.OFF)

    logger.info(
        "SparkSession creee",
        extra={
            "app_name": config.spark.app_name,
            "master":   config.spark.master,
            "version":  spark.version,
        }
    )
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Arrete proprement la SparkSession avec logging.

    A utiliser a la place de spark.stop() directement pour
    garantir que l'arret est toujours trace dans les logs.
    """
    logger.info("Arret de la SparkSession")
    spark.stop()
    logger.info("SparkSession arretee")

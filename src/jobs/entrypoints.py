# =============================================================================
# src/jobs/entrypoints.py — Points d'entrée CLI pour Databricks python_wheel_task
#
# ETAPE 13 — Databricks Asset Bundles
#
# Concept : un python_wheel_task sur Databricks appelle un "entry_point"
# enregistré dans pyproject.toml [project.scripts].
#
# Chaque fonction ici :
#   1. Parse les arguments CLI (--config, --env)
#   2. Charge la configuration AppConfig
#   3. Crée la SparkSession (getOrCreate sur Databricks → session déjà active)
#   4. Instancie et exécute le job correspondant
#
# Stratégie de chargement de la config sur Databricks :
#   - Si --config est fourni ET le fichier existe → AppConfig.from_file()
#   - Sinon → AppConfig.from_env() qui lit ENV, SPARK_MASTER, etc.
#     (ces vars sont injectées via spark_env_vars dans databricks.yml)
# =============================================================================

import argparse
import os
import sys

from utils.config import AppConfig
from utils.logger import get_logger
from utils.spark_utils import get_spark_session

logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    """Parse les arguments CLI communs à tous les entry points."""
    parser = argparse.ArgumentParser(description="Databricks PySpark job entry point")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Chemin vers le fichier de config YAML (ex: configs/dev.yaml)",
    )
    parser.add_argument(
        "--env",
        type=str,
        default=os.getenv("ENVIRONMENT", "dev"),
        choices=["dev", "staging", "prod"],
        help="Environnement de déploiement",
    )
    return parser.parse_args()


def _load_config(args: argparse.Namespace) -> AppConfig:
    """
    Charge la configuration selon le contexte d'exécution.

    Sur Databricks : le wheel est installé dans site-packages, le chemin
    relatif vers configs/ n'est plus valide. On utilise from_env() qui lit
    les variables d'environnement injectées par le cluster (databricks.yml).

    En local / CI : on peut passer --config configs/dev.yaml explicitement.
    """
    if args.config and os.path.exists(args.config):
        logger.info("Chargement config depuis fichier: %s", args.config)
        return AppConfig.from_file(args.config, env=args.env)
    logger.info("Chargement config depuis variables d'environnement (env=%s)", args.env)
    return AppConfig.from_env(args.env)


def run_bronze() -> None:
    """Entry point : ingestion Bronze (CSV → Delta)."""
    args = _parse_args()
    logger.info("Starting bronze ingestion | env=%s", args.env)
    config = _load_config(args)
    spark = get_spark_session(config)

    from jobs.etl_job import OrdersEtlJob

    job = OrdersEtlJob(config=config, spark=spark)
    job.run()
    logger.info("Bronze ingestion completed successfully")


def run_silver() -> None:
    """Entry point : transformation Silver (nettoyage + MERGE Delta)."""
    args = _parse_args()
    logger.info("Starting silver transform | env=%s", args.env)
    config = _load_config(args)
    spark = get_spark_session(config)

    from jobs.etl_job import OrdersEtlJob

    job = OrdersEtlJob(config=config, spark=spark)
    job.run()
    logger.info("Silver transform completed successfully")


def run_gold() -> None:
    """Entry point : agrégation Gold (KPIs)."""
    args = _parse_args()
    logger.info("Starting gold aggregation | env=%s", args.env)
    config = _load_config(args)
    spark = get_spark_session(config)

    from jobs.etl_job import OrdersEtlJob

    job = OrdersEtlJob(config=config, spark=spark)
    job.run()
    logger.info("Gold aggregation completed successfully")


def run_dq_report() -> None:
    """Entry point : rapport Data Quality."""
    args = _parse_args()
    logger.info("Starting DQ report | env=%s", args.env)
    config = _load_config(args)
    spark = get_spark_session(config)

    from jobs.etl_job import OrdersEtlJob

    job = OrdersEtlJob(config=config, spark=spark)
    job.run()
    logger.info("DQ report completed successfully")


if __name__ == "__main__":
    # Permet aussi l'appel direct : python -m jobs.entrypoints --entry bronze
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry", choices=["bronze", "silver", "gold", "dq_report"])
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining

    dispatch = {
        "bronze": run_bronze,
        "silver": run_silver,
        "gold": run_gold,
        "dq_report": run_dq_report,
    }
    if args.entry:
        dispatch[args.entry]()

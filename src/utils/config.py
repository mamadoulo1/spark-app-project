# =============================================================================
# src/utils/config.py
#
# Pourquoi externaliser la configuration ?
#
#   Probleme avec les valeurs hardcodees dans le code :
#     df.write.parquet("C:/Users/mamad/data/silver/orders")   <- code cassé en prod
#     spark.master = "local[*]"                               <- impossible en prod
#     null_threshold = 0.05                                   <- different par env
#
#   Solution : un fichier YAML par environnement + variables d'env pour les secrets.
#   Le code ne change jamais. Seule la config change.
#
# Strategie deux couches (priorite haute -> basse) :
#   COUCHE 2 : Variables d'environnement  (secrets, CI/CD, overrides urgents)
#   COUCHE 1 : configs/<env>.yaml         (valeurs stables, versionnees en git)
#
# Usage :
#   config = AppConfig.from_env()          # charge configs/dev.yaml par defaut
#   config = AppConfig.from_env("prod")    # charge configs/prod.yaml
#   path   = config.get_zone_path("silver", "orders")
#   -> "data/lake/silver/orders"
# =============================================================================

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from src.utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# DATACLASSES : sections de la configuration
#
# Pourquoi des dataclasses et pas un simple dict ?
#   - Autocompletion dans l'IDE : config.spark.master  (pas config["spark"]["master"])
#   - Typage : mypy detecte les erreurs de type
#   - Valeurs par defaut : pas besoin de .get() partout
#   - Immuabilite possible avec frozen=True si necessaire
# =============================================================================

@dataclass
class SparkConfig:
    """Parametres de la SparkSession."""
    app_name:                   str  = "spark-project"
    master:                     str  = "local[*]"
    log_level:                  str  = "WARN"
    shuffle_partitions:         int  = 200
    adaptive_query_execution:   bool = True
    extra_configs:    dict[str, str] = field(default_factory=dict)


@dataclass
class StorageConfig:
    """Chemins du Data Lake par zone."""
    data_lake_path:   str = ""
    checkpoint_path:  str = ""
    raw_zone:         str = "raw"
    bronze_zone:      str = "bronze"
    silver_zone:      str = "silver"
    gold_zone:        str = "gold"


@dataclass
class DataQualityConfig:
    """Parametres du moteur de qualite des donnees."""
    enabled:             bool  = True
    fail_on_error:       bool  = False
    null_threshold:      float = 0.05
    duplicate_threshold: float = 0.01


@dataclass
class AppConfig:
    """
    Configuration racine de l'application.

    Regroupe toutes les sections. Point d'entree unique pour toute
    la configuration — aucun job ne lit directement os.environ
    ou un fichier YAML en dehors de cette classe.
    """
    env:           str              = "dev"
    spark:         SparkConfig      = field(default_factory=SparkConfig)
    storage:       StorageConfig    = field(default_factory=StorageConfig)
    data_quality:  DataQualityConfig = field(default_factory=DataQualityConfig)

    # ------------------------------------------------------------------
    # Methodes de fabrique (factory methods)
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls, env: str | None = None) -> AppConfig:
        """
        Charge la config depuis configs/<env>.yaml puis applique les
        overrides des variables d'environnement par-dessus.

        Sequence de resolution :
          1. Lire $ENV (ou "dev" par defaut)
          2. Charger configs/<env>.yaml
          3. Ecraser avec les variables d'environnement

        Args:
            env: Forcer un environnement. Sinon lit $ENV.

        Returns:
            AppConfig prete a l'emploi.
        """
        # Charger le fichier .env sans ecraser les variables shell existantes
        load_dotenv(override=False)

        resolved_env = env or os.getenv("ENV", "dev")
        config_path = Path(__file__).parents[2] / "configs" / f"{resolved_env}.yaml"

        logger.debug(
            "Chargement de la configuration",
            extra={"env": resolved_env, "path": str(config_path)}
        )

        return cls._build(config_path, resolved_env)

    @classmethod
    def from_file(cls, path: str | Path, env: str = "dev") -> AppConfig:
        """
        Charge directement depuis un fichier YAML specifique.
        Utile pour les tests ou les configs non standard.
        """
        return cls._build(Path(path), env)

    @classmethod
    def _build(cls, path: Path, env: str) -> AppConfig:
        """
        Construit l'AppConfig en fusionnant le YAML et les variables d'env.

        Principe : os.getenv(KEY, yaml_value)
          -> si la variable d'env existe => elle gagne
          -> sinon la valeur du YAML est utilisee
        """
        # Lire le YAML (fichier optionnel : s'il n'existe pas on prend les defauts)
        raw: dict[str, Any] = {}
        if path.exists():
            with open(path, encoding="utf-8") as fh:
                raw = yaml.safe_load(fh) or {}
            logger.debug("YAML charge", extra={"path": str(path)})
        else:
            logger.warning(
                "Fichier de config absent, utilisation des valeurs par defaut",
                extra={"path": str(path)}
            )

        spark_raw   = raw.get("spark", {})
        storage_raw = raw.get("storage", {})
        dq_raw      = raw.get("data_quality", {})

        # ---- Section spark ----
        # os.getenv(VAR_ENV, valeur_yaml) : la variable d'env a la priorite
        spark_cfg = SparkConfig(
            app_name   = os.getenv("SPARK_APP_NAME",
                                   spark_raw.get("app_name", "spark-project")),
            master     = os.getenv("SPARK_MASTER",
                                   spark_raw.get("master", "local[*]")),
            log_level  = os.getenv("SPARK_LOG_LEVEL",
                                   spark_raw.get("log_level", "WARN")),
            shuffle_partitions       = int(spark_raw.get("shuffle_partitions", 200)),
            adaptive_query_execution = spark_raw.get("adaptive_query_execution", True),
            extra_configs            = spark_raw.get("extra_configs", {}),
        )

        # ---- Section storage ----
        storage_cfg = StorageConfig(
            data_lake_path  = os.getenv("DATA_LAKE_PATH",
                                        storage_raw.get("data_lake_path", "")),
            checkpoint_path = os.getenv("CHECKPOINT_PATH",
                                        storage_raw.get("checkpoint_path", "")),
            raw_zone    = storage_raw.get("raw_zone",    "raw"),
            bronze_zone = storage_raw.get("bronze_zone", "bronze"),
            silver_zone = storage_raw.get("silver_zone", "silver"),
            gold_zone   = storage_raw.get("gold_zone",   "gold"),
        )

        # ---- Section data_quality ----
        dq_cfg = DataQualityConfig(
            enabled             = dq_raw.get("enabled",             True),
            fail_on_error       = dq_raw.get("fail_on_error",       False),
            null_threshold      = dq_raw.get("null_threshold",      0.05),
            duplicate_threshold = dq_raw.get("duplicate_threshold", 0.01),
        )

        config = cls(
            env=env,
            spark=spark_cfg,
            storage=storage_cfg,
            data_quality=dq_cfg,
        )

        logger.info(
            "Configuration chargee",
            extra={
                "env":             env,
                "master":          spark_cfg.master,
                "data_lake_path":  storage_cfg.data_lake_path,
                "fail_on_error":   dq_cfg.fail_on_error,
            }
        )
        return config

    # ------------------------------------------------------------------
    # Methodes utilitaires
    # ------------------------------------------------------------------

    def get_zone_path(self, zone: str, dataset: str = "") -> str:
        """
        Construit le chemin complet d'un dataset dans une zone du Data Lake.

        Args:
            zone:    "raw" | "bronze" | "silver" | "gold"
            dataset: Sous-dossier optionnel (ex: "orders", "dq_results/orders")

        Returns:
            Chemin complet (ex: "data/lake/silver/orders")

        Example:
            config.storage.data_lake_path = "s3a://bucket"
            config.get_zone_path("silver", "orders")
            -> "s3a://bucket/silver/orders"
        """
        zone_name = getattr(self.storage, f"{zone}_zone", zone)
        base      = self.storage.data_lake_path.rstrip("/")
        parts     = [p for p in [base, zone_name, dataset] if p]
        return "/".join(parts)

# =============================================================================
# src/io/readers.py
#
# Abstraction de la lecture de donnees.
#
# Pourquoi une classe par format et pas spark.read directement dans les jobs ?
#
#   Sans abstraction, dans chaque job :
#     df = spark.read.option("header", True).option("inferSchema", True).csv(path)
#     # oubli de .schema() -> inferSchema en prod -> lent + instable
#     # oubli de dateFormat -> dates mal parsees
#     # options incompatibles entre dev et prod
#
#   Avec les readers :
#     df = CsvReader(spark).read(path, schema=MY_SCHEMA)
#     # options par defaut correctes appliquees automatiquement
#     # schema obligatoire -> pas d'inferSchema
#     # factory create_reader() -> le job ne connait que le format, pas la classe
#
# Pattern utilise : Strategy + Factory Method
#   Chaque format = une classe (Strategy)
#   create_reader(spark, "csv") = Factory qui retourne la bonne classe
#
# Readers disponibles :
#   ParquetReader    lecture Parquet avec schema optionnel
#   CsvReader        lecture CSV avec options securisees par defaut
#   JsonReader       lecture JSON / JSON Lines
#   DeltaReader      lecture Delta avec time travel (version ou timestamp)
#   create_reader()  factory : create_reader(spark, "delta") -> DeltaReader
# =============================================================================

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# BaseReader — contrat abstrait
# =============================================================================

class BaseReader(ABC):
    """Contrat abstrait pour tous les readers."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @abstractmethod
    def read(self, path: str, **kwargs) -> DataFrame:
        """Lit les donnees depuis path et retourne un DataFrame."""


# =============================================================================
# ParquetReader
# =============================================================================

class ParquetReader(BaseReader):
    """
    Lit des fichiers Parquet.

    Parquet est le format de stockage recommande pour les zones Bronze et Silver :
      - Binaire compresse (3-10x moins de place que CSV)
      - Schema embarque -> lecture rapide sans inferSchema
      - Supporte le pushdown de predicats (Spark ne lit que les colonnes/partitions utiles)

    Args:
        path         : Chemin local, S3 (s3a://), ADLS (abfss://) ou GCS (gs://).
        schema       : StructType explicite. Recommande pour la performance.
        merge_schema : True pour lire des partitions avec schemas heterogenes.

    Example:
        df = ParquetReader(spark).read("data/lake/bronze/orders/", schema=SCHEMA)
    """

    def read(
        self,
        path:         str,
        schema:       StructType | None = None,
        merge_schema: bool = False,
        **kwargs,
    ) -> DataFrame:
        reader = self.spark.read.option("mergeSchema", str(merge_schema).lower())
        if schema:
            reader = reader.schema(schema)
        logger.info("Lecture Parquet", extra={"path": path})
        return reader.parquet(path)


# =============================================================================
# CsvReader
# =============================================================================

class CsvReader(BaseReader):
    """
    Lit des fichiers CSV avec des options securisees par defaut.

    Options par defaut appliquees automatiquement :
      header=True, inferSchema=False (toujours passer schema=),
      dateFormat=yyyy-MM-dd, timestampFormat=yyyy-MM-dd HH:mm:ss,
      encoding=UTF-8, mode=PERMISSIVE (lignes invalides -> NULL, pas d'exception)

    IMPORTANT : ne jamais utiliser inferSchema=True en production.
    Passer toujours un schema explicite pour :
      - Eviter un double scan du fichier (inferSchema lit 2 fois)
      - Garantir des types stables d'un run a l'autre

    Args:
        path   : Chemin du fichier ou repertoire CSV.
        schema : StructType obligatoire en production.
        sep    : Separateur de colonnes (defaut: ",").
        kwargs : Options Spark suppementaires.

    Example:
        df = CsvReader(spark).read("data/raw/orders.csv", schema=RAW_SCHEMA, sep=";")
    """

    def read(
        self,
        path:   str,
        schema: StructType | None = None,
        sep:    str = ",",
        **kwargs,
    ) -> DataFrame:
        reader = (
            self.spark.read
            .option("header",          "true")
            .option("sep",             sep)
            .option("encoding",        "UTF-8")
            .option("dateFormat",      "yyyy-MM-dd")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("mode",            "PERMISSIVE")   # lignes invalides -> NULL
            .option("inferSchema",     "false")        # JAMAIS inferSchema en prod
        )
        # Appliquer les options supplementaires
        for key, value in kwargs.items():
            reader = reader.option(key, str(value))
        if schema:
            reader = reader.schema(schema)
        logger.info("Lecture CSV", extra={"path": path, "sep": sep})
        return reader.csv(path)


# =============================================================================
# JsonReader
# =============================================================================

class JsonReader(BaseReader):
    """
    Lit des fichiers JSON ou JSON Lines (un objet JSON par ligne).

    Args:
        path      : Chemin du fichier ou repertoire.
        schema    : StructType. Sans schema, Spark infere (lent, a eviter).
        multiline : True pour des fichiers JSON classiques (pas JSON Lines).

    Example:
        df = JsonReader(spark).read("data/raw/events/", schema=EVENT_SCHEMA)
    """

    def read(
        self,
        path:      str,
        schema:    StructType | None = None,
        multiline: bool = False,
        **kwargs,
    ) -> DataFrame:
        reader = self.spark.read.option("multiline", str(multiline).lower())
        for key, value in kwargs.items():
            reader = reader.option(key, str(value))
        if schema:
            reader = reader.schema(schema)
        logger.info("Lecture JSON", extra={"path": path, "multiline": multiline})
        return reader.json(path)


# =============================================================================
# DeltaReader
# =============================================================================

class DeltaReader(BaseReader):
    """
    Lit une table Delta Lake avec support du Time Travel.

    Le Time Travel est l'une des fonctionnalites cles de Delta Lake :
      - Reproductibilite : relire exactement les donnees d'un job passe
      - Debug : comparer la table avant/apres une transformation
      - Rollback : revenir a une version precedente si un bug est detecte

    Args:
        path      : Chemin de la table Delta.
        version   : Numero de version Delta (0, 1, 2...).
        timestamp : Timestamp ISO-8601 pour le time travel temporel.

    Example:
        # Lecture courante
        df = DeltaReader(spark).read("data/lake/silver/orders")

        # Time travel par version
        df = DeltaReader(spark).read("data/lake/silver/orders", version=5)

        # Time travel par timestamp
        df = DeltaReader(spark).read("data/lake/silver/orders",
                                      timestamp="2024-01-01T00:00:00")
    """

    def read(
        self,
        path:      str,
        version:   int | None = None,
        timestamp: str | None = None,
        **kwargs,
    ) -> DataFrame:
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
            logger.info("Lecture Delta (time travel version)",
                        extra={"path": path, "version": version})
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
            logger.info("Lecture Delta (time travel timestamp)",
                        extra={"path": path, "timestamp": timestamp})
        else:
            logger.info("Lecture Delta", extra={"path": path})
        return reader.load(path)


# =============================================================================
# Factory — create_reader()
# =============================================================================

_READER_REGISTRY: dict[str, type[BaseReader]] = {
    "parquet": ParquetReader,
    "csv":     CsvReader,
    "json":    JsonReader,
    "delta":   DeltaReader,
}


def create_reader(spark: SparkSession, format: str) -> BaseReader:
    """
    Cree et retourne le reader correspondant au format demande.

    Pourquoi une factory ?
      - Le job ne connait que le format ("delta"), pas la classe (DeltaReader)
      - Ajouter un nouveau format = une ligne dans _READER_REGISTRY
      - En test, on peut substituer facilement un reader par un mock

    Args:
        spark  : SparkSession active.
        format : "parquet" | "csv" | "json" | "delta"

    Returns:
        Instance du reader correspondant.

    Raises:
        ValueError : si le format n'est pas supporte.

    Example:
        reader = create_reader(spark, "csv")
        df = reader.read("data/raw/orders.csv", schema=MY_SCHEMA)
    """
    fmt = format.lower()
    if fmt not in _READER_REGISTRY:
        raise ValueError(
            f"Format '{format}' non supporte. "
            f"Formats disponibles : {sorted(_READER_REGISTRY.keys())}"
        )
    logger.debug("Reader cree", extra={"format": fmt})
    return _READER_REGISTRY[fmt](spark)

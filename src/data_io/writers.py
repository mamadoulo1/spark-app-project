# =============================================================================
# src/io/writers.py
#
# Abstraction de l'ecriture de donnees.
#
# Le probleme central : idempotence
#
#   mode("overwrite") :
#     - Ecrase toute la table, meme si le job n'a traite qu'un jour
#     - Si le job plante a mi-chemin -> donnees partiellement ecrites ou perdues
#     - Rejouer le job -> OK, mais on a peut-etre perdu des donnees d'une autre partition
#
#   mode("append") :
#     - Si le job est rejoue (orchestrateur qui retry) -> doublons garantis
#     - Impossible de corriger une ligne : il faut réécrire toute la table
#
#   Delta MERGE / upsert (la bonne solution) :
#     - Rejouer le job 2 fois = meme resultat (idempotent)
#     - Seules les lignes existantes sont mises a jour, les nouvelles sont inserees
#     - ACID : atomique, si le job plante, la table reste coherente
#     - Fonctionne sur la cle metier (order_id), pas sur une position
#
# Idempotence en pratique :
#   Lundi 10h : job OK, 1000 lignes inserees
#   Lundi 11h : orchestrateur rejoue le job (faux positif de timeout)
#   Resultat  : toujours 1000 lignes, pas 2000 (pas de doublons)
#
# Writers disponibles :
#   ParquetWriter  ecriture Parquet (append / overwrite)
#   DeltaWriter    ecriture Delta avec upsert() idempotent
#   create_writer() factory
# =============================================================================

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# BaseWriter — contrat abstrait
# =============================================================================


class BaseWriter(ABC):
    """Contrat abstrait pour tous les writers."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @abstractmethod
    def write(
        self,
        df: DataFrame,
        path: str,
        **kwargs: Any,
    ) -> None:
        """Ecrit df dans path."""


# =============================================================================
# ParquetWriter
# =============================================================================


class ParquetWriter(BaseWriter):
    """
    Ecrit un DataFrame en format Parquet.

    Args:
        df           : DataFrame a ecrire.
        path         : Chemin de destination.
        mode         : "overwrite" | "append" | "error" | "ignore"
        partition_by : Colonnes de partitionnement (ex: ["order_date", "status"])

    Quand utiliser ParquetWriter vs DeltaWriter :
      - ParquetWriter : exports one-shot, zones Raw, donnees immuables
      - DeltaWriter   : Silver / Gold ou toute table qui est mise a jour

    Example:
        ParquetWriter(spark).write(df, "data/export/orders/", mode="overwrite")
    """

    def write(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        logger.info("Ecriture Parquet", extra={"path": path, "mode": mode})
        writer.parquet(path)
        logger.info("Ecriture Parquet terminee", extra={"path": path})


# =============================================================================
# DeltaWriter
# =============================================================================


class DeltaWriter(BaseWriter):
    """
    Ecrit un DataFrame en format Delta Lake.

    Methodes :
      write()   : append ou overwrite standard
      upsert()  : MERGE idempotent sur cle metier (recommande pour Silver/Gold)

    Pourquoi Delta Lake et pas Parquet pour Silver/Gold ?
      Parquet est immuable : modifier une ligne = réécrire tout le fichier.
      Delta permet le MERGE, le DELETE, l'UPDATE sur des lignes individuelles,
      tout en garantissant les transactions ACID.
    """

    def write(
        self,
        df: DataFrame,
        path: str,
        mode: str = "append",
        partition_by: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Ecriture Delta standard (append ou overwrite).

        Pour une ecriture idempotente, preferer upsert().
        """
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        logger.info("Ecriture Delta", extra={"path": path, "mode": mode})
        writer.save(path)
        logger.info("Ecriture Delta terminee", extra={"path": path})

    def upsert(
        self,
        df: DataFrame,
        path: str,
        merge_keys: list[str],
        update_columns: list[str] | None = None,
    ) -> None:
        """
        Upsert idempotent via Delta MERGE.

        Si la table n'existe pas encore : premiere ecriture en mode "overwrite".
        Si la table existe : MERGE sur les cles specifiees.

        Logique SQL generee :
            MERGE INTO target USING source
            ON target.order_id = source.order_id
            WHEN MATCHED     THEN UPDATE SET *         (ou colonnes specifiees)
            WHEN NOT MATCHED THEN INSERT *

        Idempotence :
            Rejouer l'upsert avec les memes donnees = meme resultat.
            Aucun doublon, aucune perte de donnee.

        Args:
            df             : DataFrame source (nouvelles donnees).
            path           : Chemin de la table Delta cible.
            merge_keys     : Cle(s) de correspondance source/cible.
                             Ex: ["order_id"] ou ["order_id", "line_id"]
            update_columns : Colonnes a mettre a jour lors d'un match.
                             None = mettre a jour toutes les colonnes.

        Example:
            DeltaWriter(spark).upsert(
                df=silver_df,
                path="data/lake/silver/orders",
                merge_keys=["order_id"],
            )
        """
        from delta.tables import DeltaTable

        # Condition de jointure : target.key = source.key pour chaque cle
        merge_condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

        logger.info("Upsert Delta (MERGE)", extra={"path": path, "merge_keys": merge_keys})

        if DeltaTable.isDeltaTable(self.spark, path):
            # La table existe -> MERGE
            delta_table = DeltaTable.forPath(self.spark, path)
            merge_builder = (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()  # mise a jour de toutes les colonnes
                .whenNotMatchedInsertAll()  # insertion si nouvelle ligne
            )
            merge_builder.execute()
            logger.info("MERGE Delta termine", extra={"path": path})
        else:
            # Premiere ecriture : creer la table Delta
            logger.info("Table Delta absente, creation par ecriture initiale", extra={"path": path})
            (df.write.format("delta").mode("overwrite").save(path))
            logger.info("Table Delta creee", extra={"path": path})


# =============================================================================
# Factory — create_writer()
# =============================================================================

_WRITER_REGISTRY: dict[str, type[BaseWriter]] = {
    "parquet": ParquetWriter,
    "delta": DeltaWriter,
}


def create_writer(spark: SparkSession, format: str) -> BaseWriter:
    """
    Cree et retourne le writer correspondant au format demande.

    Args:
        spark  : SparkSession active.
        format : "parquet" | "delta"

    Returns:
        Instance du writer correspondant.

    Example:
        writer = create_writer(spark, "delta")
        writer.upsert(df, path, merge_keys=["order_id"])
    """
    fmt = format.lower()
    if fmt not in _WRITER_REGISTRY:
        raise ValueError(
            f"Format '{format}' non supporte. "
            f"Formats disponibles : {sorted(_WRITER_REGISTRY.keys())}"
        )
    logger.debug("Writer cree", extra={"format": fmt})
    return _WRITER_REGISTRY[fmt](spark)

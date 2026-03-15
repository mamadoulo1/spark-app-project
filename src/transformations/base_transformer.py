# =============================================================================
# src/transformations/base_transformer.py
#
# Pourquoi des classes de transformation ?
#
#   SANS transformers — une seule grande fonction dans run() :
#
#     def transform(df):
#         df = df.select(...)
#         df = df.dropDuplicates(["order_id"])
#         df = df.filter(F.col("order_id").isNotNull())
#         df = df.withColumn("status", F.upper(F.col("status")))
#         df = df.withColumn("total",  F.col("qty") * F.col("price"))
#         df = df.withColumn("_ts",    F.current_timestamp())
#         ...20 autres lignes...
#         return df
#
#   Problemes :
#     - Impossible de tester TrimStrings sans tester aussi DropDuplicates
#     - Impossible de reutiliser une etape dans un autre job
#     - Ajouter/retirer une etape = risquer de casser les etapes voisines
#     - Difficile a lire : 30 lignes sans structure claire
#
#   AVEC transformers — pipeline compose :
#
#     pipeline = compose(
#         TrimStrings(),
#         DropDuplicates(subset=["order_id"]),
#         CastColumns({"order_date": "date"}),
#         AddAuditColumns(pipeline_name="orders_etl"),
#     )
#     result = pipeline(df)
#
#   Benefices :
#     - Chaque transformer est testable en isolation (1 classe = 1 test)
#     - Reutilisable dans n'importe quel job
#     - Lisible comme une recette : on voit l'ordre et le but de chaque etape
#     - Ajouter/retirer une ligne = ajouter/retirer une etape
#
# Pattern utilise : Composite + Chain of Responsibility
#   Chaque transformer est un maillon.
#   compose() les enchaine en une pipeline sequentielle.
# =============================================================================

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable

from pyspark.sql import DataFrame

from utils.logger import get_logger

logger = get_logger(__name__)


class BaseTransformer(ABC):
    """
    Contrat abstrait pour toutes les transformations DataFrame.

    Une sous-classe implemente transform(df) -> df.
    La methode __call__ permet d'utiliser l'instance comme une fonction :

        transformer = MonTransformer()
        result = transformer(df)        <- appelle transformer.transform(df)

    C'est ce qui permet a compose() de fonctionner :
        pipeline = compose(A(), B(), C())
        result = pipeline(df)           <- A(df) -> B -> C -> result
    """

    def __init__(self, **options: Any) -> None:
        self.options = options

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique la transformation sur df et retourne le resultat.

        Regles a respecter pour chaque transformer :
          - Stateless : pas de SparkSession, pas de config en attribut
          - Idempotent si possible : appliquer deux fois = meme resultat
          - Retourner TOUJOURS un DataFrame (meme si vide)
          - Ne jamais modifier df en place (Spark est immutable de toute facon)
        """

    def __call__(self, df: DataFrame) -> DataFrame:
        """
        Rend l'instance appelable comme une fonction : transformer(df).

        Logue automatiquement chaque application pour la tracabilite.
        """
        logger.debug(
            "Application du transformer",
            extra={"transformer": self.__class__.__name__, "options": self.options},
        )
        result = self.transform(df)
        logger.debug("Transformer termine", extra={"transformer": self.__class__.__name__})
        return result


def compose(*transformers: BaseTransformer) -> Callable[[DataFrame], DataFrame]:
    """
    Compose plusieurs transformers en une seule pipeline sequentielle.

    Les transformers sont appliques de gauche a droite (premier argument = premier).

    Args:
        *transformers : Instances de transformers dans l'ordre d'application.

    Returns:
        Un callable df -> df qui applique tous les transformers en sequence.

    Example:
        pipeline = compose(
            TrimStrings(),
            DropDuplicates(subset=["order_id"]),
            AddAuditColumns(pipeline_name="orders_etl", env="prod"),
        )
        silver_df = pipeline(bronze_df)

    Note:
        compose() retourne une fonction ordinaire, pas un BaseTransformer.
        On peut donc composer des pipelines de pipelines :
            cleaning   = compose(TrimStrings(), DropNullKeys(["id"]))
            enrichment = compose(CastColumns({...}), AddAuditColumns(...))
            full       = compose(cleaning, enrichment)
    """

    def _pipeline(df: DataFrame) -> DataFrame:
        for transformer in transformers:
            df = transformer(df)
        return df

    return _pipeline

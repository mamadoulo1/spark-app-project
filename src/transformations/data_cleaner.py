# =============================================================================
# src/transformations/data_cleaner.py
#
# Bibliotheque de transformers reutilisables pour le nettoyage des donnees.
#
# Chaque classe est independante, testable en isolation, et utilisable
# dans n'importe quel job via compose().
#
# Transformers disponibles :
#   TrimStrings()                      - supprime espaces debut/fin (toutes colonnes string)
#   DropDuplicates(subset)             - supprime les doublons
#   DropNullKeys(key_columns)          - supprime les lignes avec cles NULL
#   CastColumns(cast_map)              - caste les colonnes vers les types cibles
#   RenameColumns(rename_map)          - renomme des colonnes
#   NormaliseStrings(columns, case)    - normalise la casse (upper/lower/title)
#   ComputeDerivedColumns(expressions) - calcule de nouvelles colonnes via SQL
#   FilterRows(condition)              - filtre par expression SQL
#   AddAuditColumns(pipeline, env)     - ajoute _processing_ts, _pipeline_name, _env
# =============================================================================

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType

from src.transformations.base_transformer import BaseTransformer
from src.utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# 1. TrimStrings
# =============================================================================


class TrimStrings(BaseTransformer):
    """
    Supprime les espaces (et autres whitespace) en debut et fin de toutes
    les colonnes de type StringType.

    Pourquoi c'est important :
      "  PENDING  " != "PENDING"
      -> joins qui echouent, doublons non detectes, filtres qui ratent

    Idempotent : appliquer deux fois = meme resultat.

    Example:
        TrimStrings()(df)
        # " Alice " -> "Alice"  pour toutes les colonnes string
    """

    def transform(self, df: DataFrame) -> DataFrame:
        string_columns = [
            field.name for field in df.schema.fields if isinstance(field.dataType, StringType)
        ]
        for col_name in string_columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
        logger.debug("TrimStrings applique", extra={"colonnes_traitees": len(string_columns)})
        return df


# =============================================================================
# 2. DropDuplicates
# =============================================================================


class DropDuplicates(BaseTransformer):
    """
    Supprime les lignes dupliquees.

    Sans subset : compare toutes les colonnes (doublons exacts).
    Avec subset  : compare seulement les colonnes specifiees (cle metier).

    En cas de doublons sur la cle, la premiere occurrence est conservee.
    Pour conserver la plus recente, il faut d'abord trier par date.

    Args:
        subset : Liste de colonnes pour la cle de deduplication.
                 None = deduplication sur toutes les colonnes.

    Example:
        DropDuplicates()                     # doublons exacts
        DropDuplicates(subset=["order_id"])  # deduplique sur la cle metier
    """

    def __init__(self, subset: list[str] | None = None) -> None:
        super().__init__(subset=subset)
        self.subset = subset

    def transform(self, df: DataFrame) -> DataFrame:
        before = df.count()
        result = df.dropDuplicates(self.subset) if self.subset else df.dropDuplicates()
        after = result.count()
        dropped = before - after
        if dropped > 0:
            logger.warning("Doublons supprimes", extra={"dropped": dropped, "subset": self.subset})
        else:
            logger.debug("Aucun doublon detecte", extra={"subset": self.subset})
        return result


# =============================================================================
# 3. DropNullKeys
# =============================================================================


class DropNullKeys(BaseTransformer):
    """
    Supprime toute ligne ou au moins une des colonnes cles est NULL.

    Pourquoi c'est necessaire :
      - Une ligne sans order_id ne peut pas etre fusionnee avec la Silver
      - Un NULL dans une cle etrangere rompt l'integrite referentielle
      - Les checks DQ signaleront le probleme mais on retire d'abord

    Args:
        key_columns : Colonnes qui ne doivent pas etre NULL.

    Example:
        DropNullKeys(key_columns=["order_id", "customer_id"])
    """

    def __init__(self, key_columns: list[str]) -> None:
        super().__init__(key_columns=key_columns)
        self.key_columns = key_columns

    def transform(self, df: DataFrame) -> DataFrame:
        # Condition : au moins une cle est NULL
        null_condition = F.lit(False)
        for col in self.key_columns:
            if col in df.columns:
                null_condition = null_condition | F.col(col).isNull()

        before = df.count()
        result = df.filter(~null_condition)
        after = result.count()
        dropped = before - after

        if dropped > 0:
            logger.warning(
                "Lignes avec cles NULL supprimees",
                extra={"dropped": dropped, "key_columns": self.key_columns},
            )
        return result


# =============================================================================
# 4. CastColumns
# =============================================================================


class CastColumns(BaseTransformer):
    """
    Caste les colonnes vers les types cibles specifies.

    Les colonnes absentes du DataFrame sont ignorees silencieusement
    (pas d'erreur si une colonne optionnelle n'est pas presente).

    Args:
        cast_map : Dictionnaire {nom_colonne: type_spark_en_string}.

    Types courants :
        "integer", "long", "double", "decimal(18,4)",
        "date", "timestamp", "boolean", "string"

    Example:
        CastColumns({
            "quantity":   "integer",
            "unit_price": "decimal(18,4)",
            "order_date": "date",
            "is_active":  "boolean",
        })
    """

    def __init__(self, cast_map: dict[str, str]) -> None:
        super().__init__(cast_map=cast_map)
        self.cast_map = cast_map

    def transform(self, df: DataFrame) -> DataFrame:
        existing_columns = set(df.columns)
        for col_name, target_type in self.cast_map.items():
            if col_name in existing_columns:
                df = df.withColumn(col_name, F.col(col_name).cast(target_type))
                logger.debug("Colonne castee", extra={"colonne": col_name, "type": target_type})
            else:
                logger.debug("Colonne absente, cast ignore", extra={"colonne": col_name})
        return df


# =============================================================================
# 5. RenameColumns
# =============================================================================


class RenameColumns(BaseTransformer):
    """
    Renomme des colonnes selon un mapping.

    Les colonnes absentes du DataFrame sont ignorees silencieusement.

    Args:
        rename_map : Dictionnaire {ancien_nom: nouveau_nom}.

    Example:
        RenameColumns({
            "cust_id": "customer_id",
            "qty":     "quantity",
        })
    """

    def __init__(self, rename_map: dict[str, str]) -> None:
        super().__init__(rename_map=rename_map)
        self.rename_map = rename_map

    def transform(self, df: DataFrame) -> DataFrame:
        for old_name, new_name in self.rename_map.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        return df


# =============================================================================
# 6. NormaliseStrings
# =============================================================================


class NormaliseStrings(BaseTransformer):
    """
    Normalise la casse des colonnes string specifiees.

    Args:
        columns : Colonnes a normaliser. None = toutes les StringType.
        case    : "upper" | "lower" | "title"

    Example:
        NormaliseStrings(case="upper")                   # toutes les string
        NormaliseStrings(columns=["status"], case="upper")
        NormaliseStrings(columns=["name"], case="title") # "jean" -> "Jean"
    """

    def __init__(
        self,
        columns: list[str] | None = None,
        case: str = "upper",
    ) -> None:
        super().__init__(columns=columns, case=case)
        self.columns = columns
        self.case = case.lower()

    def transform(self, df: DataFrame) -> DataFrame:
        target_columns = self.columns or [
            f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
        ]
        func = {"upper": F.upper, "lower": F.lower, "title": F.initcap}.get(self.case)
        if func is None:
            raise ValueError(f"case doit etre upper/lower/title, recu : {self.case!r}")
        for col_name in target_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, func(F.col(col_name)))
        return df


# =============================================================================
# 7. ComputeDerivedColumns
# =============================================================================


class ComputeDerivedColumns(BaseTransformer):
    """
    Ajoute ou remplace des colonnes via des expressions SQL Spark.

    Utilise selectExpr ou withColumn avec expr() pour evaluer
    des expressions SQL directement.

    Args:
        expressions : Dictionnaire {nom_colonne: expression_sql}.

    Example:
        ComputeDerivedColumns({
            "total_amount":  "CAST(quantity AS DECIMAL(18,4)) * unit_price",
            "full_name":     "CONCAT(first_name, ' ', last_name)",
            "year_month":    "date_format(order_date, 'yyyy-MM')",
            "is_high_value": "total_amount > 1000",
            "currency":      "UPPER(COALESCE(currency, 'USD'))",
        })

    Note : les colonnes sont calculees dans l'ordre du dictionnaire.
    Une expression peut referencer une colonne calculee plus haut.
    """

    def __init__(self, expressions: dict[str, str]) -> None:
        super().__init__(expressions=expressions)
        self.expressions = expressions

    def transform(self, df: DataFrame) -> DataFrame:
        for col_name, expr_str in self.expressions.items():
            df = df.withColumn(col_name, F.expr(expr_str))
            logger.debug("Colonne calculee", extra={"colonne": col_name, "expression": expr_str})
        return df


# =============================================================================
# 8. FilterRows
# =============================================================================


class FilterRows(BaseTransformer):
    """
    Filtre les lignes selon une expression SQL Spark.

    Args:
        condition : Expression SQL pour filter() / where().

    Example:
        FilterRows("status != 'CANCELLED'")
        FilterRows("quantity > 0 AND unit_price > 0")
        FilterRows("order_date >= '2024-01-01'")
    """

    def __init__(self, condition: str) -> None:
        super().__init__(condition=condition)
        self.condition = condition

    def transform(self, df: DataFrame) -> DataFrame:
        before = df.count()
        result = df.filter(F.expr(self.condition))
        after = result.count()
        logger.debug(
            "Lignes filtrees",
            extra={
                "condition": self.condition,
                "avant": before,
                "apres": after,
                "filtrees": before - after,
            },
        )
        return result


# =============================================================================
# 9. AddAuditColumns
# =============================================================================


class AddAuditColumns(BaseTransformer):
    """
    Ajoute des colonnes de tracabilite (audit trail) au DataFrame.

    Colonnes ajoutees :
        _processing_ts  : timestamp UTC au moment du traitement
        _pipeline_name  : nom du job/pipeline qui a produit cette ligne
        _env            : environnement d'execution (dev/staging/prod)

    Ces colonnes permettent de repondre a : "Quand cette ligne a-t-elle
    ete ecrite ? Par quel pipeline ? Dans quel environnement ?"

    Args:
        pipeline_name : Nom du pipeline (ex: "orders_etl").
        env           : Environnement (ex: "dev", "prod").

    Example:
        AddAuditColumns(pipeline_name="orders_etl", env="prod")
    """

    def __init__(self, pipeline_name: str, env: str = "dev") -> None:
        super().__init__(pipeline_name=pipeline_name, env=env)
        self.pipeline_name = pipeline_name
        self.env = env

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_processing_ts", F.current_timestamp())
            .withColumn("_pipeline_name", F.lit(self.pipeline_name))
            .withColumn("_env", F.lit(self.env))
        )

# =============================================================================
# src/schemas/schemas.py
#
# Schemas centralises du projet.
#
# Pourquoi centraliser les schemas ?
#   Sans schemas centralises, chaque job redefinit le meme StructType :
#     - Duplication : un champ ajoute doit etre mis a jour partout
#     - Incoherence : un job utilise IntegerType, un autre StringType pour qty
#     - Maintenabilite : impossible de savoir "ou est la source de verite" du schema
#
#   Avec ce module :
#     from schemas.schemas import RAW_ORDERS_SCHEMA
#     # La definition est a un seul endroit, importee par tous les jobs et tests
# =============================================================================

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Schema brut des commandes (zone Raw / Bronze)
# Correspond au CSV source : data/orders.csv
RAW_ORDERS_SCHEMA = StructType(
    [
        StructField(
            "order_id", StringType(), nullable=True
        ),  # nullable=True : donnees brutes peuvent contenir des nulls
        StructField(
            "customer_id", StringType(), nullable=True
        ),  # DropNullKeys les rejettera en Silver
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(18, 4), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
    ]
)

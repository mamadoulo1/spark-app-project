"""
Shared pytest fixtures for the full test suite.

The SparkSession fixture is session-scoped to avoid the overhead of creating
a new context for every test — PySpark startup takes several seconds.
"""

from __future__ import annotations

import os
import sys

# Ces variables doivent etre definies AVANT la creation de la SparkSession.
# Sans elles, les Python workers tentent de se connecter via host.docker.internal
# (ajout automatique par Docker Desktop) au lieu de 127.0.0.1 -> SocketTimeout.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")

import pytest
from pyspark.sql import SparkSession

from src.utils.spark_utils import get_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped SparkSession for tests.

    Uses a local[1] master with minimal configuration so tests run fast
    without any cluster or cloud dependency.
    """
    session = get_spark_session(testing=True)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_orders_data() -> list[tuple]:
    """
    Donnees de test au format tuple, alignees sur RAW_ORDERS_SCHEMA.

    Colonnes : order_id, customer_id, product_id, quantity, unit_price,
               status, order_date, channel

    Types Python requis par Spark :
      - DecimalType  -> decimal.Decimal
      - DateType     -> datetime.date
    """
    import datetime
    from decimal import Decimal

    return [
        (
            "ORD-001",
            "CUST-A",
            "PROD-X",
            2,
            Decimal("19.99"),
            "DELIVERED",
            datetime.date(2024, 1, 15),
            "ONLINE",
        ),
        (
            "ORD-002",
            "CUST-B",
            "PROD-Y",
            1,
            Decimal("99.50"),
            "SHIPPED",
            datetime.date(2024, 1, 16),
            "MOBILE",
        ),
        (
            "ORD-003",
            "CUST-C",
            "PROD-Z",
            3,
            Decimal("5.00"),
            "PENDING",
            datetime.date(2024, 1, 17),
            "STORE",
        ),
    ]

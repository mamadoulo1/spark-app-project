"""
Integration tests for the OrdersEtlJob.

These tests spin up a real (local) SparkSession and exercise the full
Extract -> Transform -> Validate pipeline against in-memory DataFrames.
No external storage or Kafka required.
"""
from __future__ import annotations

import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from src.jobs.etl_job import OrdersEtlJob
from src.schemas.schemas import RAW_ORDERS_SCHEMA
from src.utils.config import AppConfig


def _make_df(spark: SparkSession, data: list[tuple]):
    """Helper: cree un DataFrame avec RAW_ORDERS_SCHEMA depuis une liste de tuples."""
    return spark.createDataFrame(data, schema=RAW_ORDERS_SCHEMA)


@pytest.fixture
def job(spark: SparkSession) -> OrdersEtlJob:
    config = AppConfig()
    config.data_quality.fail_on_error = False
    return OrdersEtlJob(config=config, spark=spark)


@pytest.mark.integration
class TestOrdersEtlJobTransform:
    """Test the transform step of the ETL job in isolation."""

    def test_transform_adds_total_amount(
        self, spark: SparkSession, job: OrdersEtlJob, sample_orders_data: list[tuple]
    ) -> None:
        df = _make_df(spark, sample_orders_data)
        result = job.transform(df)
        assert "total_amount" in result.columns
        rows = {row["order_id"]: row for row in result.collect()}
        # ORD-001 : qty=2, price=19.99 -> total=39.98
        assert float(rows["ORD-001"]["total_amount"]) == pytest.approx(39.98, abs=0.01)

    def test_transform_adds_audit_columns(
        self, spark: SparkSession, job: OrdersEtlJob, sample_orders_data: list[tuple]
    ) -> None:
        df = _make_df(spark, sample_orders_data)
        result = job.transform(df)
        assert "_processing_ts" in result.columns
        assert "_pipeline_name" in result.columns

    def test_transform_uppercases_status(
        self, spark: SparkSession, job: OrdersEtlJob, sample_orders_data: list[tuple]
    ) -> None:
        # Statut en minuscules -> doit etre converti en majuscules
        data = [("ORD-001", "CUST-A", "PROD-X", 2, Decimal("19.99"), "delivered",
                 datetime.date(2024, 1, 15), "ONLINE")]
        df = _make_df(spark, data)
        result = job.transform(df)
        row = result.collect()[0]
        assert row["status"] == "DELIVERED"

    def test_transform_drops_null_order_id(
        self, spark: SparkSession, job: OrdersEtlJob, sample_orders_data: list[tuple]
    ) -> None:
        # Une ligne avec order_id=None doit etre rejetee
        data = list(sample_orders_data) + [
            (None, "CUST-D", "PROD-W", 1, Decimal("10.00"), "PENDING",
             datetime.date(2024, 1, 18), "ONLINE")
        ]
        df = _make_df(spark, data)
        result = job.transform(df)
        assert result.count() == len(sample_orders_data)

    def test_transform_deduplicates_by_order_id(
        self, spark: SparkSession, job: OrdersEtlJob, sample_orders_data: list[tuple]
    ) -> None:
        # Doublon de ORD-001 -> doit etre deduplique
        data = list(sample_orders_data) + [sample_orders_data[0]]
        df = _make_df(spark, data)
        result = job.transform(df)
        assert result.count() == len(sample_orders_data)


@pytest.mark.integration
class TestOrdersEtlJobValidation:
    """Test the DQ validation step."""

    def test_validate_passes_clean_data(
        self, spark: SparkSession, job: OrdersEtlJob, sample_orders_data: list[tuple]
    ) -> None:
        df = _make_df(spark, sample_orders_data)
        transformed = job.transform(df)
        job.validate(transformed)  # ne doit pas lever d'exception

    def test_validate_increments_metrics(
        self, spark: SparkSession, job: OrdersEtlJob, sample_orders_data: list[tuple]
    ) -> None:
        df = _make_df(spark, sample_orders_data)
        transformed = job.transform(df)
        job.validate(transformed)
        assert job.metrics.dq_checks_passed > 0

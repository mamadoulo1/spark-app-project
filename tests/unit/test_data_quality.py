"""Unit tests for the data quality module."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src.quality.data_quality import (
    AcceptedValuesCheck,
    DataQualityChecker,
    DataQualityError,
    NotNullCheck,
    RangeCheck,
    RowCountCheck,
    Severity,
    UniquenessCheck,
)


@pytest.mark.unit
class TestNotNullCheck:
    def test_passes_when_no_nulls(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), ("B",)], ["id"])
        result = NotNullCheck("id").run(df, "test")
        assert result.passed

    def test_fails_when_nulls_present(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), (None,)], ["id"])
        result = NotNullCheck("id").run(df, "test")
        assert not result.passed
        assert result.failing_rows == 1

    def test_passes_with_threshold(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), (None,), ("C",), ("D",)], ["id"])
        result = NotNullCheck("id", threshold=0.30).run(df, "test")
        assert result.passed  # 25% nulls ≤ 30% threshold

    def test_fails_above_threshold(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(None,), (None,), ("C",)], ["id"])
        result = NotNullCheck("id", threshold=0.10).run(df, "test")
        assert not result.passed


@pytest.mark.unit
class TestUniquenessCheck:
    def test_passes_for_unique_values(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), ("B",), ("C",)], ["id"])
        result = UniquenessCheck("id").run(df, "test")
        assert result.passed

    def test_fails_for_duplicates(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), ("A",), ("B",)], ["id"])
        result = UniquenessCheck("id").run(df, "test")
        assert not result.passed
        assert result.failing_rows == 1

    def test_composite_key_uniqueness(self, spark: SparkSession) -> None:
        data = [("A", 1), ("A", 2), ("B", 1)]
        df = spark.createDataFrame(data, ["id", "sub_id"])
        result = UniquenessCheck(["id", "sub_id"]).run(df, "test")
        assert result.passed


@pytest.mark.unit
class TestAcceptedValuesCheck:
    def test_passes_all_accepted(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("OPEN",), ("CLOSED",)], ["status"])
        result = AcceptedValuesCheck("status", ["OPEN", "CLOSED"]).run(df, "test")
        assert result.passed

    def test_fails_with_unexpected_value(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("OPEN",), ("INVALID",)], ["status"])
        result = AcceptedValuesCheck("status", ["OPEN", "CLOSED"]).run(df, "test")
        assert not result.passed
        assert result.failing_rows == 1


@pytest.mark.unit
class TestRowCountCheck:
    def test_passes_within_range(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(i,) for i in range(10)], ["val"])
        result = RowCountCheck(min_rows=5, max_rows=20).run(df, "test")
        assert result.passed

    def test_fails_below_minimum(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["val"])
        result = RowCountCheck(min_rows=5).run(df, "test")
        assert not result.passed

    def test_fails_above_maximum(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(i,) for i in range(100)], ["val"])
        result = RowCountCheck(max_rows=50).run(df, "test")
        assert not result.passed


@pytest.mark.unit
class TestRangeCheck:
    def test_passes_within_range(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(5,), (10,), (15,)], ["val"])
        result = RangeCheck("val", min_val=1, max_val=20).run(df, "test")
        assert result.passed

    def test_fails_below_min(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(-1,), (5,)], ["val"])
        result = RangeCheck("val", min_val=0).run(df, "test")
        assert not result.passed

    def test_fails_above_max(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(5,), (200,)], ["val"])
        result = RangeCheck("val", max_val=100).run(df, "test")
        assert not result.passed


@pytest.mark.unit
class TestDataQualityChecker:
    def test_all_pass_no_assertion_error(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), ("B",)], ["id"])
        checker = DataQualityChecker("test_dataset")
        checker.add_check(NotNullCheck("id"))
        checker.add_check(UniquenessCheck("id"))
        checker.run(df)
        checker.assert_no_failures()  # should not raise

    def test_fail_severity_raises(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), (None,)], ["id"])
        checker = DataQualityChecker("test_dataset")
        checker.add_check(NotNullCheck("id", severity=Severity.FAIL))
        checker.run(df)
        with pytest.raises(DataQualityError):
            checker.assert_no_failures()

    def test_warn_severity_does_not_raise(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), (None,)], ["id"])
        checker = DataQualityChecker("test_dataset")
        checker.add_check(NotNullCheck("id", severity=Severity.WARN))
        checker.run(df)
        checker.assert_no_failures()  # WARN should not raise

    def test_summary_counts(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",), ("A",)], ["id"])
        checker = DataQualityChecker("test_dataset")
        checker.add_check(NotNullCheck("id"))
        checker.add_check(UniquenessCheck("id"))
        checker.run(df)
        summary = checker.summary
        assert summary["total_checks"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1

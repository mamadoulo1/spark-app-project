"""Unit tests for transformation modules."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.transformations.data_cleaner import (
    AddAuditColumns,
    CastColumns,
    ComputeDerivedColumns,
    DropDuplicates,
    DropNullKeys,
    FilterRows,
    RenameColumns,
    TrimStrings,
)


@pytest.mark.unit
class TestDropDuplicates:
    def test_removes_exact_duplicates(self, spark: SparkSession) -> None:
        data = [("A", 1), ("A", 1), ("B", 2)]
        df = spark.createDataFrame(data, ["id", "val"])
        result = DropDuplicates()(df)
        assert result.count() == 2

    def test_removes_duplicates_by_subset(self, spark: SparkSession) -> None:
        data = [("A", 1), ("A", 2), ("B", 3)]
        df = spark.createDataFrame(data, ["id", "val"])
        result = DropDuplicates(subset=["id"])(df)
        assert result.count() == 2

    def test_no_duplicates_unchanged(self, spark: SparkSession) -> None:
        data = [("A", 1), ("B", 2), ("C", 3)]
        df = spark.createDataFrame(data, ["id", "val"])
        result = DropDuplicates()(df)
        assert result.count() == 3


@pytest.mark.unit
class TestDropNullKeys:
    def test_drops_rows_with_null_key(self, spark: SparkSession) -> None:
        data = [("A", "X"), (None, "Y"), ("C", None)]
        df = spark.createDataFrame(data, ["id", "name"])
        result = DropNullKeys(key_columns=["id"])(df)
        assert result.count() == 2

    def test_multiple_key_columns(self, spark: SparkSession) -> None:
        data = [("A", "X"), (None, "Y"), ("C", None)]
        df = spark.createDataFrame(data, ["id", "name"])
        result = DropNullKeys(key_columns=["id", "name"])(df)
        assert result.count() == 1

    def test_no_nulls_unchanged(self, spark: SparkSession) -> None:
        data = [("A", "X"), ("B", "Y")]
        df = spark.createDataFrame(data, ["id", "name"])
        result = DropNullKeys(key_columns=["id"])(df)
        assert result.count() == 2


@pytest.mark.unit
class TestCastColumns:
    def test_cast_string_to_integer(self, spark: SparkSession) -> None:
        data = [("1",), ("2",), ("3",)]
        df = spark.createDataFrame(data, ["qty"])
        result = CastColumns({"qty": "integer"})(df)
        field = next(f for f in result.schema.fields if f.name == "qty")
        assert isinstance(field.dataType, IntegerType)

    def test_missing_column_is_skipped(self, spark: SparkSession) -> None:
        data = [("A",)]
        df = spark.createDataFrame(data, ["id"])
        # Should not raise
        result = CastColumns({"nonexistent": "integer"})(df)
        assert result.columns == ["id"]


@pytest.mark.unit
class TestTrimStrings:
    def test_trims_leading_trailing_whitespace(self, spark: SparkSession) -> None:
        data = [("  hello  ",), ("  world",)]
        df = spark.createDataFrame(data, ["name"])
        result = TrimStrings()(df)
        values = [row["name"] for row in result.collect()]
        assert values == ["hello", "world"]

    def test_non_string_columns_unchanged(self, spark: SparkSession) -> None:
        schema = StructType(
            [StructField("id", IntegerType()), StructField("name", StringType())]
        )
        data = [(1, "  alice  "), (2, "bob  ")]
        df = spark.createDataFrame(data, schema)
        result = TrimStrings()(df)
        ids = [row["id"] for row in result.collect()]
        assert ids == [1, 2]


@pytest.mark.unit
class TestRenameColumns:
    def test_renames_existing_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",)], ["old_name"])
        result = RenameColumns({"old_name": "new_name"})(df)
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_missing_source_column_is_skipped(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",)], ["id"])
        result = RenameColumns({"nonexistent": "new_name"})(df)
        assert result.columns == ["id"]


@pytest.mark.unit
class TestComputeDerivedColumns:
    def test_adds_new_column_from_expression(self, spark: SparkSession) -> None:
        data = [(2, 10.0), (3, 5.0)]
        df = spark.createDataFrame(data, ["qty", "price"])
        result = ComputeDerivedColumns({"total": "qty * price"})(df)
        assert "total" in result.columns
        totals = [row["total"] for row in result.collect()]
        assert totals == [20.0, 15.0]

    def test_multiple_expressions(self, spark: SparkSession) -> None:
        data = [("hello",)]
        df = spark.createDataFrame(data, ["text"])
        result = ComputeDerivedColumns(
            {"upper_text": "UPPER(text)", "len_text": "LENGTH(text)"}
        )(df)
        row = result.collect()[0]
        assert row["upper_text"] == "HELLO"
        assert row["len_text"] == 5


@pytest.mark.unit
class TestAddAuditColumns:
    def test_adds_required_audit_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",)], ["id"])
        result = AddAuditColumns(pipeline_name="test_job", env="dev")(df)
        assert "_processing_ts" in result.columns
        assert "_pipeline_name" in result.columns
        assert "_env" in result.columns

    def test_env_and_pipeline_name_values(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("A",)], ["id"])
        result = AddAuditColumns(pipeline_name="my_job", env="prod")(df)
        row = result.collect()[0]
        assert row["_pipeline_name"] == "my_job"
        assert row["_env"] == "prod"


@pytest.mark.unit
class TestFilterRows:
    def test_filters_by_condition(self, spark: SparkSession) -> None:
        data = [(1,), (2,), (3,)]
        df = spark.createDataFrame(data, ["val"])
        result = FilterRows("val > 1")(df)
        assert result.count() == 2

    def test_no_rows_pass_filter(self, spark: SparkSession) -> None:
        data = [(1,), (2,)]
        df = spark.createDataFrame(data, ["val"])
        result = FilterRows("val > 100")(df)
        assert result.count() == 0

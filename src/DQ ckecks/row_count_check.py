# Databricks notebook source
from datetime import datetime
from pyspark.sql import Row
import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def row_count_exact_check_log(df, table_path: str, layer: str, batch_id: str,
                              logging_table: str, expected_rows: int):
    """
    Check that the DataFrame has exactly `expected_rows` rows using Great Expectations,
    log result to DQ log Delta table, and raise an error if the check fails.

    Args:
        df (DataFrame): PySpark DataFrame to check
        table_path (str): Table or dataset path
        layer (str): Layer name (Bronze/Silver/Gold)
        batch_id (str): Batch ID for this run
        logging_table (str): Full catalog name of DQ log table
        expected_rows (int): Exact number of expected rows
    """

    now = datetime.now()
    year, month, day = now.year, now.month, now.day

    # Wrap PySpark DF as GE dataset
    ge_df = ge.dataset.SparkDFDataset(df)

    # Run exact row count expectation
    result = ge_df.expect_table_row_count_to_equal(expected_rows)

    # Determine status
    status = "PASS" if result["success"] else "FAIL"
    observed_value = f"{result['result']['observed_value']} rows"
    error_msg = "" if status == "PASS" else f"Row count {observed_value} does not match expected {expected_rows}"

    # Build DQ log row
    dq_row = Row(
        table_path=table_path,
        layer=layer,
        batch_id=batch_id,
        check_name="row_count_exact_check",
        check_type="validation",
        expected_value=f"Exactly {expected_rows} rows",
        observed_value=observed_value,
        status=status,
        error_message=error_msg,
        check_timestamp=now,
        year=year,
        month=month,
        day=day
    )

    # Append to Delta DQ log table
    spark.createDataFrame([dq_row]).write.format("delta") \
        .mode("append").saveAsTable(logging_table)

    # Raise error if check failed
    if status == "FAIL":
        raise ValueError(f"Exact row count check failed: {error_msg}")

    return dq_row
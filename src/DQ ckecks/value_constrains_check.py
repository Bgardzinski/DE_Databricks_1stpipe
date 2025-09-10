# Databricks notebook source
from datetime import datetime
from pyspark.sql import Row
import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def column_non_negative_check_log(df, table_path: str, layer: str, batch_id: str,
                                  logging_table: str, columns_to_check: list):
    """
    Check that values in specified columns are >= 0 using Great Expectations,
    log result to DQ log Delta table, and raise an error if any column fails.

    Args:
        df (DataFrame): PySpark DataFrame to check
        table_path (str): Table or dataset path
        layer (str): Layer name (Bronze/Silver/Gold)
        batch_id (str): Batch ID for this run
        logging_table (str): Full catalog name of DQ log table
        columns_to_check (list): List of column names to check
    """

    now = datetime.now()
    year, month, day = now.year, now.month, now.day

    dq_rows = []
    failed_columns = []

    # Wrap PySpark DF as GE dataset
    ge_df = ge.dataset.SparkDFDataset(df)

    for col in columns_to_check:
        # Run non-negative value check
        result = ge_df.expect_column_values_to_be_between(col, min_value=0, max_value=None)

        # Determine status
        status = "PASS" if result["success"] else "FAIL"
        observed_value = f"{result['result']['unexpected_count']} invalid values"
        error_msg = "" if status == "PASS" else f"{observed_value} in column {col} (< 0)"

        # Track failed columns
        if status == "FAIL":
            failed_columns.append(col)

        # Build DQ log row
        dq_row = Row(
            table_path=table_path,
            layer=layer,
            batch_id=batch_id,
            check_name=f"non_negative_check_{col}",
            check_type="validation",
            expected_value="Values >= 0",
            observed_value=observed_value,
            status=status,
            error_message=error_msg,
            check_timestamp=now,
            year=year,
            month=month,
            day=day
        )

        dq_rows.append(dq_row)

    # Append all results to Delta DQ log table
    spark.createDataFrame(dq_rows).write.format("delta") \
        .mode("append").saveAsTable(logging_table)

    # Raise error if any column failed
    if failed_columns:
        failed_str = ", ".join(failed_columns)
        raise ValueError(f"Non-negative value check failed for columns: {failed_str}")

    return dq_rows
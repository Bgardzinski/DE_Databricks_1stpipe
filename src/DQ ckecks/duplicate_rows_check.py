# Databricks notebook source
from datetime import datetime
from pyspark.sql import Row
import great_expectations as ge
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def multi_column_uniqueness_check_log(df, table_path: str, layer: str, batch_id: str,
                                      logging_table: str, columns_to_check: list):
    """
    Check for duplicates across multiple columns using GE,
    log results to DQ log Delta table, and raise an error if duplicates exist.

    Args:
        df (DataFrame): PySpark DataFrame to check
        table_path (str): Table or dataset path
        layer (str): Layer name (Bronze/Silver/Gold)
        batch_id (str): Batch ID for this run
        logging_table (str): Full catalog name of DQ log table
        columns_to_check (list): List of column names to define uniqueness
                                 (combination of columns)
    """

    now = datetime.now()
    year, month, day, hour = now.year, now.month, now.day, now.hour

    dq_rows = []

    # Wrap PySpark DF as GE dataset
    ge_df = ge.dataset.SparkDFDataset(df)

    # Run multi-column uniqueness check
    result = ge_df.expect_multicolumn_values_to_be_unique(columns_to_check)

    status = "PASS" if result["success"] else "FAIL"
    observed_value = f"{result['result']['unexpected_count']} duplicate rows"
    error_msg = "" if status == "PASS" else f"{observed_value} for columns {columns_to_check}"

    # Build DQ log row
    dq_row = Row(
        table_path=table_path,
        layer=layer,
        batch_id=batch_id,
        check_name=f"multi_column_uniqueness_{'_'.join(columns_to_check)}",
        check_type="validation",
        expected_value=f"Unique combination of {columns_to_check}",
        observed_value=observed_value,
        status=status,
        error_message=error_msg,
        check_timestamp=now,
        year=year,
        month=month,
        day=day
    )

    dq_rows.append(dq_row)

    # Append to Delta DQ log table
    spark.createDataFrame(dq_rows).write.format("delta") \
        .mode("append").saveAsTable(logging_table)

    # Raise error if duplicates exist
    if status == "FAIL":
        raise ValueError(f"Multi-column uniqueness check failed for columns: {columns_to_check}")

    return dq_rows
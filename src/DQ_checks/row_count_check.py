from datetime import datetime
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


def row_count_exact_check_log(df, table_path: str, layer: str,
                              logging_table: str, expected_rows: int):
    """
    Check that the DataFrame has exactly `expected_rows` rows using native Spark,
    log result to DQ log Delta table, and raise an error if the check fails.

    Args:
        df (DataFrame): PySpark DataFrame to check
        table_path (str): Table or dataset path
        layer (str): Layer name (Bronze/Silver/Gold)
        logging_table (str): Full catalog name of DQ log table
        expected_rows (int): Exact number of expected rows
    """

    now = datetime.now()
    year, month, day = now.year, now.month, now.day

    # --- Native Spark row count ---
    actual_count = df.count()

    # Determine status
    status = "PASS" if actual_count == expected_rows else "FAIL"
    observed_value = f"{actual_count} rows"
    error_msg = "" if status == "PASS" else f"Row count {observed_value} does not match expected {expected_rows}"

    # Build DQ log row
    dq_row = Row(
        table_path=table_path,
        layer=layer,
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

    dq_row_df = spark.createDataFrame([dq_row]) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("month", col("month").cast("int")) \
        .withColumn("day", col("day").cast("int"))

    dq_row_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(logging_table)
    # Raise error if check failed
    if status == "FAIL":
        raise ValueError(f"Exact row count check failed: {error_msg}")

    return dq_row

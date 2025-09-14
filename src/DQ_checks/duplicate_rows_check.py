from datetime import datetime
from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


def multi_column_uniqueness_check_log(df, table_path: str, layer: str, logging_table: str, columns_to_check: list):
    """
    Check for duplicates across multiple columns using native PySpark,
    log results to DQ log Delta table, and raise an error if duplicates exist.

    Args:
        df (DataFrame): PySpark DataFrame to check
        table_path (str): Table or dataset path
        layer (str): Layer name (Bronze/Silver/Gold)
        logging_table (str): Full catalog name of DQ log table
        columns_to_check (list): List of column names to define uniqueness
                                 (combination of columns)
    """

    now = datetime.now()
    year, month, day, hour = now.year, now.month, now.day, now.hour

    # Count total rows vs distinct rows
    total_count = df.count()
    distinct_count = df.select(columns_to_check).distinct().count()

    duplicate_count = total_count - distinct_count

    # Determine status
    status = "PASS" if duplicate_count == 0 else "FAIL"
    observed_value = f"{duplicate_count} duplicate rows"
    error_msg = "" if status == "PASS" else f"{observed_value} for columns {columns_to_check}"

    # Build DQ log row
    dq_row = Row(
        table_path=table_path,
        layer=layer,
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

    dq_row_df = spark.createDataFrame([dq_row]) \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("month", col("month").cast("int")) \
        .withColumn("day", col("day").cast("int"))

    dq_row_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(logging_table)

    # Raise error if duplicates exist
    if status == "FAIL":
        raise ValueError(f"Multi-column uniqueness check failed for columns: {columns_to_check}")

    return dq_row

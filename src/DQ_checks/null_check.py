from datetime import datetime
from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


def null_check_log(df, table_path: str, layer: str,
                   logging_table: str, columns_to_check: list):
    """
    Check for nulls in specified columns using native PySpark,
    log result to DQ log Delta table, and raise an error if any column fails.

    Args:
        df (DataFrame): PySpark DataFrame to check
        table_path (str): Table or dataset path
        layer (str): Layer name (Bronze/Silver/Gold)
        logging_table (str): Full catalog name of DQ log table
        columns_to_check (list): List of column names to check for nulls
    """

    now = datetime.now()
    year, month, day = now.year, now.month, now.day

    dq_rows = []
    failed_columns = []

    for col in columns_to_check:
        # Count nulls in the column
        null_count = df.filter(F.col(col).isNull()).count()

        # Determine status
        status = "PASS" if null_count == 0 else "FAIL"
        observed_value = f"{null_count} nulls"
        error_msg = "" if status == "PASS" else f"{observed_value} in column {col}"

        # Track failed columns
        if status == "FAIL":
            failed_columns.append(col)

        # Build DQ log row
        dq_row = Row(
            table_path=table_path,
            layer=layer,
            check_name=f"null_check_{col}",
            check_type="validation",
            expected_value="No nulls",
            observed_value=observed_value,
            status=status,
            error_message=error_msg,
            check_timestamp=now,
            year=year,
            month=month,
            day=day
        )

        dq_rows.append(dq_row)

    dq_df = spark.createDataFrame(dq_rows)

    # Explicitly cast partition columns to int
    dq_df = (
        dq_df.withColumn("year", F.col("year").cast("int"))
            .withColumn("month", F.col("month").cast("int"))
            .withColumn("day", F.col("day").cast("int"))
    )

    # Append all results to Delta DQ log table
    dq_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(logging_table)

    # Raise error if any check failed
    if failed_columns:
        failed_str = ", ".join(failed_columns)
        raise ValueError(f"Null check failed for columns: {failed_str}")

    return dq_rows

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def expected_rows(df, symbols_file="/FileStore/tables/Symbols.txt"):
    """
    Calculate expected number of rows based on the number of symbols
    and distinct extract_time values in df.
    
    :param df: Spark DataFrame containing 'extract_time' column
    :param symbols_file: path to the symbols file in DBFS
    :return: expected number of rows (int)
    """

    # Load symbols
    symbols_df = spark.read.csv(symbols_file, header=False).toDF("symbol")
    
    # Remove empty rows and duplicates
    symbols_df = symbols_df.filter(col("symbol").isNotNull()).distinct()
    
    # Count symbols
    num_symbols = symbols_df.count()
    
    # Count distinct extract_time
    num_extract_time = df.select("extract_time").distinct().count()
    
    # Expected rows
    return num_symbols * num_extract_time
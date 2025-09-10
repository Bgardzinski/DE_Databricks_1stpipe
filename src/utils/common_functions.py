from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType,LongType
from functools import reduce
from operator import and_

def cast_ints(bronze_df, int_columns):

    df_cleaned = bronze_df
    for col_name in int_columns:
        df_cleaned = df_cleaned.withColumn(col_name, col(col_name).cast(LongType()))
        
    return df_cleaned


def filter_not_null(df, columns):

    conditions = [col(c).isNotNull() for c in columns]
    combined_condition = reduce(and_, conditions)

    return df.filter(combined_condition)


def drop_duplicates_on_columns(df, columns):
    return df.dropDuplicates(columns)
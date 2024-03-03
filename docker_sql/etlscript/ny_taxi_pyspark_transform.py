import pyspark 
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

# data_path = '/home/idowuileks/Desktop/tech_project/nyc_taxi_project/docker_sql/etl_script/data/2023_data/yellow_tripdata_2023-01.parquet'


# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


# df = spark.read.option("header","true").format("parquet").load(data_path)
from pyspark.sql import functions as F
from pyspark.sql.functions import year, date_format,col
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, dayofweek, date_format, last_day

# def get_date_info_pyspark(df: pyspark.sql.DataFrame, column:str, new_column_prefix:str) -> pyspark.sql.DataFrame:
#     """
#     Extracts various date information from a PySpark DataFrame column.

#     Args:
#         df: The PySpark DataFrame.
#         date_column: he name of the date column.
#         new_column_prefix: The prefix for the new columns containing date information.

#     Returns:
#         The modified DataFrame with new columns containing year, month, day, hour, minute,
#         day of week, day name, month-end flag, and year-end flag.
#     """

#     #   df = df.withColumn(f"{new_column_prefix}_year", F.year(F.col(column)))
#     #   df = df.withColumn(f"{new_column_prefix}_month", F.month(F.col(column)))
#     #   df = df.withColumn(f"{new_column_prefix}_day", F.dayofmonth(F.col(column)))
#     #   df = df.withColumn(f"{new_column_prefix}_hour", F.hour(F.col(column)))
#     #   df = df.withColumn(f"{new_column_prefix}_minute", F.minute(F.col(column)))
#     #   df = df.withColumn(f"{new_column_prefix}_day_of_week", F.dayofweek(column))
#     #   df = df.withColumn(f"{new_column_prefix}_day_name", F.date_format(column, "EEEE"))
#     #   df = df.withColumn(f"{new_column_prefix}_is_month_end", F.last_day(column) == column)
#     #   df = df.withColumn(f"{new_column_prefix}_is_year_end", F.last_day(column) == F.date_add(
#     #       F.last_day(column), F.months(1) - F.dayofmonth(column)
    
#     #   ))
    
#     # df = (df.withColumn(f"{new_column_prefix}_year", F.year(F.col(date_column)))
#     #         .withColumn(f"{new_column_prefix}_month", F.month(F.col(date_column)))
#     #         .withColumn(f"{new_column_prefix}_day", F.dayofmonth(F.col(date_column)))
#     #         .withColumn(f"{new_column_prefix}_hour", F.hour(F.col(date_column)))
#     #         .withColumn(f"{new_column_prefix}_minute", F.minute(F.col(date_column)))
#     #         .withColumn(f"{new_column_prefix}_day_of_week", F.dayofweek(date_column))
#     #         .withColumn(f"{new_column_prefix}_day_name", F.date_format(date_column, "EEEE"))
#     #         .withColumn(f"{new_column_prefix}_is_month_end", F.last_day(date_column) == date_column)
#     #       )
#     # return df

#     df = df.withColumn(f"{new_column_prefix}_year", F.year(F.col(column)))
#     df = df.withColumn(f"{new_column_prefix}_month", F.month(F.col(column)))
#     df = df.withColumn(f"{new_column_prefix}_day", F.dayofmonth(F.col(column)))
#     df = df.withColumn(f"{new_column_prefix}_hour", F.hour(F.col(column)))
#     df = df.withColumn(f"{new_column_prefix}_minute", F.minute(F.col(column)))
#     df = df.withColumn(f"{new_column_prefix}_day_of_week", F.dayofweek(column))
#     df = df.withColumn(f"{new_column_prefix}_day_name", F.date_format(column, "EEEE"))
#     df = df.withColumn(f"{new_column_prefix}_is_month_end", F.last_day(column) == column)
# #   df = df.withColumn(f"{new_column_prefix}_is_year_end", F.last_day(column) == F.date_add(
# #       F.last_day(column), F.months(1) - F.dayofmonth(column)
# #   ))
#     return df
  

# Example usage
# df_pyspark = spark.createDataFrame([("2024-01-31 23:59:59",)], ["date"])
# df_with_date_info = get_date_info_pyspark(df_pyspark, "date", "date_info")
# df_with_date_info.show()
# df_time_table_pickup = get_date_info_pyspark(df,'tpep_dropoff_datetime','pickup')

# df_time_table_pickup.show()




def join_pick_drop(column_a, column_b):
    """
    Concatenates two columns after casting them to StringType.
    
    Args:
    column_a: Name of the first column
    column_b: Name of the second column
    
    Returns:
    Concatenated string of the two columns
    """
    return F.concat_ws("_", col(column_a).cast(StringType()), col(column_b).cast(StringType()))
    # return col(column_a).cast(StringType()) + "_" + col(column_b).cast(StringType())



# def transform_data(data):
#     """
#     This function takes a dataframe 'data' and performs a series of transformations on it to create 
#     dimension_table_data, df_time_table_dropoff, and fact_data_table. It returns these three dataframes.
#     """

#     lower_column_name = [F.col(col_name).alias(col_name.lower()) for col_name in data.columns]
#     data = data.select(*lower_column_name)

#     fact_columns = ['tpep_dropoff_datetime','tpep_pickup_datetime','passenger_count', 'trip_distance','fare_amount', 'extra',
#         'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
#         'total_amount', 'congestion_surcharge', 'airport_fee']

#     dimension_table_data = ['tpep_dropoff_datetime','tpep_pickup_datetime','ratecodeid','store_and_fwd_flag','pulocationid','dolocationid','payment_type']

#     dimension_table_data = data.select(*dimension_table_data)

#     time_table_data = data.select('tpep_dropoff_datetime','tpep_pickup_datetime')

#     df_time_table_pickup = get_date_info_pyspark(time_table_data, 'tpep_pickup_datetime', 'pickup')

#     df_time_table_dropoff = get_date_info_pyspark(df_time_table_pickup, 'tpep_dropoff_datetime', 'dropoff')

#     df_time_table_dropoff = df_time_table_dropoff.withColumn('datetime_id', join_pick_drop('tpep_pickup_datetime', 'tpep_dropoff_datetime'))

#     fact_data_table = data.select(*fact_columns)

#     fact_data_table = fact_data_table.withColumn('datetime_id', join_pick_drop('tpep_pickup_datetime', 'tpep_dropoff_datetime'))
#     fact_data_table = fact_data_table.selectExpr(*fact_data_table.columns[::-1])
#     fact_data_table = fact_data_table.drop('tpep_dropoff_datetime', 'tpep_pickup_datetime')

#     dimension_table_data = dimension_table_data.withColumn('datetime_id', join_pick_drop('tpep_pickup_datetime', 'tpep_dropoff_datetime'))
#     dimension_table_data = dimension_table_data.select('datetime_id', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid', 'payment_type')

#     ordered_column_time = ['datetime_id','tpep_dropoff_datetime',
#     'tpep_pickup_datetime',
#     'pickup_year',
#     'pickup_month',
#     'pickup_day',
#     'pickup_hour',
#     'pickup_minute',
#     'pickup_day_of_week',
#     'pickup_day_name',
#     'pickup_is_month_end',
#     'dropoff_year',
#     'dropoff_month',
#     'dropoff_day',
#     'dropoff_hour',
#     'dropoff_minute',
#     'dropoff_day_of_week',
#     'dropoff_day_name',
#     'dropoff_is_month_end']

#     df_time_table_dropoff = df_time_table_dropoff.selectExpr(*ordered_column_time)

#     df_time_table_dropoff = df_time_table_dropoff.withColumn('tpep_dropoff_datetime', F.col('tpep_dropoff_datetime').cast('timestamp'))
#     df_time_table_dropoff = df_time_table_dropoff.withColumn('tpep_pickup_datetime', F.col('tpep_pickup_datetime').cast('timestamp'))

#     df_time_table_dropoff = df_time_table_dropoff.dropDuplicates(subset=['datetime_id'])
#     fact_data_table = fact_data_table.dropDuplicates(subset=['datetime_id'])
#     dimension_table_data = dimension_table_data.dropDuplicates(subset=['datetime_id'])
#     # duplicates = dimension_table_data.groupBy("datetime_id").count().filter(col("count") > 1)

#     # if duplicates.count() > 0:
#     #     print("Duplicate values found in 'datetime_id':")
#     #     duplicates.show()
#         # Handle duplicates accordingly, e.g., log, raise exception, or clean data


#     return dimension_table_data, df_time_table_dropoff, fact_data_table
def get_date_info(df, column, new_column_prefix):
    df = df.withColumn(f'{new_column_prefix}_year', year(col(column)))
    df = df.withColumn(f'{new_column_prefix}_month', month(col(column)))
    df = df.withColumn(f'{new_column_prefix}_day', dayofmonth(col(column)))
    df = df.withColumn(f'{new_column_prefix}_hour', hour(col(column)))
    df = df.withColumn(f'{new_column_prefix}_minute', minute(col(column)))
    df = df.withColumn(f'{new_column_prefix}_day_of_week', dayofweek(col(column)))
    df = df.withColumn(f'{new_column_prefix}_day_name', date_format(col(column), 'EEEE'))
    df = df.withColumn(f'{new_column_prefix}_is_month_end', date_format(last_day(col(column)), 'yyyy-MM-dd') == col(column))
    # df = df.withColumn(f'{new_column_prefix}_is_year_end', year(last_day(col(column))) == year(col(column)))

    return df
def transform_data(data):
    # spark = SparkSession.builder.appName("data_transform").getOrCreate()

    lower_column_name = [col(col_name).alias(col_name.lower()) for col_name in data.columns]
    data = data.select(*lower_column_name)

    fact_columns = ['tpep_dropoff_datetime','tpep_pickup_datetime','passenger_count', 'trip_distance','fare_amount', 'extra',
        'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount', 'congestion_surcharge', 'airport_fee']

    dimension_table_data = ['tpep_dropoff_datetime','tpep_pickup_datetime','ratecodeid','store_and_fwd_flag','pulocationid','dolocationid','payment_type']

    dimension_table_data = data.select(*dimension_table_data)

    time_table_data = data.select('tpep_dropoff_datetime','tpep_pickup_datetime')

    df_time_table_pickup = get_date_info(time_table_data, 'tpep_pickup_datetime', 'pickup')

    df_time_table_dropoff = get_date_info(df_time_table_pickup, 'tpep_dropoff_datetime', 'dropoff')

    # df_time_table_dropoff.show()

    df_time_table_dropoff = df_time_table_dropoff.withColumn('datetime_id', join_pick_drop("tpep_pickup_datetime", "tpep_dropoff_datetime"))

    fact_data_table = data.select(fact_columns)

    fact_data_table = fact_data_table.withColumn('datetime_id', join_pick_drop('tpep_pickup_datetime', 'tpep_dropoff_datetime'))
    fact_data_table = fact_data_table.select(fact_data_table.columns[::-1])
    fact_data_table = fact_data_table.drop('tpep_dropoff_datetime', 'tpep_pickup_datetime')
    # print(dimension_table_data.schema)

    dimension_table_data = dimension_table_data.withColumn('datetime_id', join_pick_drop('tpep_pickup_datetime', 'tpep_dropoff_datetime'))
    # print(dimension_table_data.schema)
    dimension_table_data = dimension_table_data.select('datetime_id', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid', 'payment_type')

    ordered_column_time = ['datetime_id','tpep_dropoff_datetime',
    'tpep_pickup_datetime',
    'pickup_year',
    'pickup_month',
    'pickup_day',
    'pickup_hour',
    'pickup_minute',
    'pickup_day_of_week',
    'pickup_day_name',
    'pickup_is_month_end',
    'dropoff_year',
    'dropoff_month',
    'dropoff_day',
    'dropoff_hour',
    'dropoff_minute',
    'dropoff_day_of_week',
    'dropoff_day_name',
    'dropoff_is_month_end']

    df_time_table_dropoff = df_time_table_dropoff.select(ordered_column_time)

    df_time_table_dropoff = df_time_table_dropoff.withColumn('tpep_dropoff_datetime', df_time_table_dropoff.tpep_dropoff_datetime.cast('timestamp'))
    df_time_table_dropoff = df_time_table_dropoff.withColumn('tpep_pickup_datetime', df_time_table_dropoff.tpep_pickup_datetime.cast('timestamp'))

    df_time_table_dropoff = df_time_table_dropoff.dropDuplicates(subset=['datetime_id'])
    fact_data_table = fact_data_table.dropDuplicates(subset=['datetime_id'])
    dimension_table_data = dimension_table_data.dropDuplicates(subset=['datetime_id'])

    return dimension_table_data, df_time_table_dropoff, fact_data_table
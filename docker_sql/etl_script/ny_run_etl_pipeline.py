# testing github
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pyspark 
from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql.functions import year, date_format,col
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, dayofweek, date_format, last_day

# from ny_taxi_transform import transform_data
from etl_script.ny_taxi_pyspark_transform import transform_data
from etl_script.ny_taxi_extract import download_store_data
# from ny_taxi_load_sql import create_table_load_data, create_dimension_table_statement, create_time_table_statement, fact_table_creation_sql
from etl_script.ny_taxi_load_sql import fact_dimension_sql_statement,create_table_load_data
import pandas as pd
import os
cwd = os.getcwd()
def download_load_data(year, month, parent_folder_path,spark,jdbc_uri,db_user,db_password_quote,engine,drop_table,ti,schema_name,create_new_folder=True):
    """
    Download and load data for the given year and month into the specified parent folder path. 
    Optionally create a new folder if it does not exist. 
    """
    data_path, folder_path = download_store_data(year, month, parent_folder_path=parent_folder_path, create_new_folder=create_new_folder)
    print(data_path)
    df = spark.read.option("header","true").format("parquet").load(data_path,inferSchema="true")
    # data = pd.read_parquet(data_path)

    # create_dimension_table_statement, create_fact_table_statement = fact_dimension_sql_statement('datetime_trip_table')
    create_time_table_statement , create_dimension_table_statement, create_fact_table_statement = fact_dimension_sql_statement('datetime_trip_table','DIMENSION_TAXI_TABLE','fact_table_taxi_ride',db_user,schema_name,drop_table)


    dimension_table_data, time_table_data, fact_data_table = transform_data(df)

    create_table_load_data(time_table_data, 'datetime_trip_table',schema_name,create_time_table_statement,engine,jdbc_uri,db_user,db_password_quote)

    create_table_load_data(dimension_table_data,'DIMENSION_TAXI_TABLE',schema_name,create_dimension_table_statement,engine,jdbc_uri,db_user,db_password_quote)

    # # create_table_load_data(time_table_data, 'datetime_trip_table', create_time_table_statement)


    create_table_load_data(fact_data_table, 'fact_table_taxi_ride',schema_name,create_fact_table_statement,engine,jdbc_uri,db_user,db_password_quote)

    ti.xcom_push(key='data_path', value=data_path)
    ti.xcom_push(key='folder_path', value=folder_path)

def download_load_data_test(year, month, parent_folder_path,spark,jdbc_uri,db_user,db_password_quote,engine,drop_table,schema_name,create_new_folder=False):
    """
    Download and load data for the given year and month into the specified parent folder path. 
    Optionally create a new folder if it does not exist. 
    """
    data_path, folder_path = download_store_data(year, month, parent_folder_path=parent_folder_path, create_new_folder=create_new_folder)
    print(data_path)
    df = spark.read.option("header","true").format("parquet").load(data_path,inferSchema="true")
    # data = pd.read_parquet(data_path)

    # create_dimension_table_statement, create_fact_table_statement = fact_dimension_sql_statement('datetime_trip_table')
    create_time_table_statement , create_dimension_table_statement, create_fact_table_statement = fact_dimension_sql_statement('datetime_trip_table_test','DIMENSION_TAXI_TABLE_test','fact_table_taxi_ride_test',db_user,schema_name,drop_table)


    dimension_table_data, time_table_data, fact_data_table = transform_data(df)

    create_table_load_data(time_table_data, 'datetime_trip_table_test',schema_name,create_time_table_statement,engine,jdbc_uri,db_user,db_password_quote,test=True)

    create_table_load_data(dimension_table_data,'DIMENSION_TAXI_TABLE_test',schema_name,create_dimension_table_statement,engine,jdbc_uri,db_user,db_password_quote,test=True)

    # # create_table_load_data(time_table_data, 'datetime_trip_table', create_time_table_statement)


    create_table_load_data(fact_data_table, 'fact_table_taxi_ride_test',schema_name,create_fact_table_statement,engine,jdbc_uri,db_user,db_password_quote,test=True)

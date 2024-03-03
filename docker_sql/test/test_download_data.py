# import etl_script
# # from ..etl_script import ny_taxi_extract
import sys

# Verify the path you are adding to sys.path is correct
sys.path.insert(0, './docker_sql')
from docker_sql.ny_run_etl_pipeline import download_load_data_test
# from docker_sql.etl_script.ny_taxi_transform import transform_data
from docker_sql.ny_taxi_extract import download_store_data
# # from ..etl_script.ny_taxi_load_sql import create_table_load_data, create_dimension_table_statement, create_time_table_statement, fact_table_creation_sql, engine
# # from ..etl_script.ny_taxi_load_sql import fact_dimension_sql_statement, create_time_table_statement, engine, create_table_load_data
import pandas as pd
import os
from datetime import datetime, timedelta
import pyspark 
from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql.functions import year, date_format,col
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, dayofweek, date_format, last_day

# from ny_taxi_transform import transform_data
from docker_sql.ny_taxi_pyspark_transform import transform_data
# from etl_script.ny_taxi_extract import download_store_data
# from ny_taxi_load_sql import create_table_load_data, create_dimension_table_statement, create_time_table_statement, fact_table_creation_sql
from docker_sql.ny_taxi_load_sql import fact_dimension_sql_statement,create_table_load_data
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

db_host = os.getenv('POSTGRES_HOST', 'localhost')
db_user = os.getenv('POSTGRES_USER', 'idowuuser')
db_password = os.getenv('POSTGRES_PASSWORD')
db_name = os.getenv('POSTGRES_DBNAME', 'ny_taxi_database')
db_port = os.getenv('POSTGRES_PORT', '5433')
# db_host = "localhost"
# db_user = "idowuuser"
# db_password = "passwordguddy"
db_password_quote = quote_plus(db_password)
# db_password_quote = db_password
db_name = "ny_taxi_database"
# db_port = 5433
database_uri = 'postgresql+psycopg2://' + db_user + ':' + db_password_quote + '@' + db_host + ':' + str(db_port) + '/' + db_name
jdbc_uri =f"jdbc:postgresql://{db_host}:{str(db_port)}/{db_name}"
engine = create_engine(database_uri)
spark = SparkSession.builder.appName("hello").config("spark.master", "local[*]").config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# # import connectorx as cx
# import connectorx as cx

# connect_string = 'postgresql://idowu_user:idowupassword@localhost:5434/nyc_taxi'
# create_time_table_statement , create_dimension_table_statement, create_fact_table_statement = fact_dimension_sql_statement('datetime_trip_table_test','DIMENSION_TAXI_TABLE_test','fact_table_taxi_ride_test')
# data_path = download_store_data(2023, 3)

# data = pd.read_parquet(data_path)


# dimension_table_data, time_table_data, fact_data_table = transform_data(data)

# create_table_load_data(time_table_data, 'datetime_trip_table', create_time_table_statement)

# create_table_load_data(dimension_table_data,'DIMENSION_TAXI_TABLE',create_dimension_table_statement)

# # create_table_load_data(time_table_data, 'datetime_trip_table', create_time_table_statement)

#creating a function for get failed and non-failed test
# class MyPlugin:
#     def __init__(self):
#         self.passed = 0
#         self.failed = 0

#     def pytest_runtest_logreport(self, report):
#         if report.when != 'call':
#             return
#         if report.passed:
#             self.passed += 1
#         elif report.failed:
#             self.failed +=1 
#     def pytest_sessionfinish(self, session, exitstatus):
#         print(self.passed, self.failed, sep=',')

# @pytest.mark.tryfirst
# def pytest_configure(config):
#     config.pluginmanager.register(MyPlugin(), 'my_plugin')
# create_table_load_data(fact_data_table, 'fact_table_taxi_ride', fact_table_creation_sql)

def read_files_from_database_df(schema_name,table_name):
    sql_statement = f"select * from {schema_name}.{table_name} LIMIT 5"
    data = pd.read_sql(sql_statement, engine.connect())

    return len(data)

def test_download_file(tmp_path):
    tmp_dir = tmp_path / "test_folder"
    tmp_dir.mkdir()
    data_path, folder_path = download_store_data(2023,3,parent_folder_path=tmp_dir, create_new_folder=False)

    data = pd.read_parquet(data_path)

    assert len(data) != 0


def test_load_into_database(tmp_path):
    
    tmp_dir = tmp_path/"test_folder_load"
    tmp_dir.mkdir()
    tmp_dir = tmp_path
    data_path, folder_path = download_store_data(2023, 3,parent_folder_path=tmp_dir, create_new_folder=False)
    data = pd.read_parquet(data_path)
    schema_name = 'raw_test'
    data = data.head(5)
    drop_table = True
    download_load_data_test(2023, 3, tmp_dir,spark,jdbc_uri,db_user,db_password_quote,engine,drop_table,schema_name,create_new_folder=False)
    
#     dimension_table_data, time_table_data, fact_data_table = transform_data(data)

#     create_table_load_data(time_table_data, 'datetime_trip_table_test', create_time_table_statement,test=True)

#     create_table_load_data(dimension_table_data,'DIMENSION_TAXI_TABLE_test',create_dimension_table_statement,test=True)

#     create_table_load_data(fact_data_table, 'fact_table_taxi_ride_test', create_fact_table_statement,test=True)
    
    

    assert read_files_from_database_df(schema_name,'datetime_trip_table_test') == 5

    assert read_files_from_database_df(schema_name,'DIMENSION_TAXI_TABLE_test') == 5

    assert read_files_from_database_df(schema_name,'fact_table_taxi_ride_test') == 5

# import tempfile

# dir_path = tempfile.mkdtemp()


# test_load_into_database(dir_path)

    
    

# def download_store_data(year, month, parent_folder_path, create_new_folder=True):
#     if create_new_folder:
#         file_path = os.path.join(f'{parent_folder_path}', str(year)+"_data")
#         file_path = str(file_path)
#         os.makedirs(file_path, exist_ok=True)
    
#         url ="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s-%02d.parquet"%(year,month)
#         # print(file_path)
#         os.system(f"wget {url} -O {file_path}/yellow_tripdata_{year}-{month:02d}.parquet")

#         # data = pd.read_parquet("yellow_tripdata_%s-%02d.parquet"%(year,month))

#         return f"{file_path}/yellow_tripdata_{year}-{month:02d}.parquet"
#     else:
#         url ="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s-%02d.parquet"%(year,month)
#         os.system(f"wget {url} -O {parent_folder_path}/yellow_tripdata_{year}-{month:02d}.parquet")
#         return f"{parent_folder_path}/yellow_tripdata_{year}-{month:02d}.parquet"








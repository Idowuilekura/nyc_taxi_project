from ny_taxi_transform import transform_data
from ny_taxi_extract import download_store_data
# from ny_taxi_load_sql import create_table_load_data, create_dimension_table_statement, create_time_table_statement, fact_table_creation_sql, engine
from ny_taxi_load_sql import fact_dimension_sql_statement, create_time_table_statement, engine, create_table_load_data
import pandas as pd
import os
cwd = os.getcwd()

data_path = download_store_data(2023, 3, parent_folder_path=str(cwd), create_new_folder=True )

data = pd.read_parquet(data_path)

# create_dimension_table_statement, create_fact_table_statement = fact_dimension_sql_statement('datetime_trip_table')
create_time_table_statement , create_dimension_table_statement, create_fact_table_statement = fact_dimension_sql_statement('datetime_trip_table_test','DIMENSION_TAXI_TABLE_test','fact_table_taxi_ride_test')


dimension_table_data, time_table_data, fact_data_table = transform_data(data)

create_table_load_data(time_table_data, 'datetime_trip_table', create_time_table_statement)

create_table_load_data(dimension_table_data,'DIMENSION_TAXI_TABLE',create_dimension_table_statement)

# # create_table_load_data(time_table_data, 'datetime_trip_table', create_time_table_statement)


create_table_load_data(fact_data_table, 'fact_table_taxi_ride', create_fact_table_statement)
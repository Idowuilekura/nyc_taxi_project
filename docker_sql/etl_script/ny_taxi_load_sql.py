# from ny_taxi_transform import df_time_table_dropoff, fact_data_table, dimension_table_data
from sqlalchemy import create_engine,sql
import pandas as pd
engine = create_engine('postgresql+psycopg2://idowu_user:idowupassword@postgres_service_new:5432/nyc_taxi')

try:
    engine.connect()
except:
    engine = create_engine('postgresql+psycopg2://idowu_user:idowupassword@postgres_service_new:5432')
    con = engine.connect()
    con.execute("commit")
    con.exec_driver_sql("create database nyc_taxi")
    con.close()

engine = create_engine('postgresql+psycopg2://idowu_user:idowupassword@postgres_service_new:5432/nyc_taxi')
def row_to_tuple(row, column):
    # values = [ df_time_table_dropoff[col][i] for col in ordered_column_time]
    values = [str(row[col]) if 'tpep' in col else row[col] for col in column]
    return tuple(values)

create_time_table_statement = """CREATE TABLE IF NOT EXISTS datetime_trip_table (datetime_id VARCHAR PRIMARY KEY, 
                                                                           tpep_dropoff_datetime TIMESTAMP,
                                                                           tpep_pickup_datetime TIMESTAMP,
                                                                           pickup_year INT NOT NULL,
                                                                            pickup_month INT NOT NULL,
                                                                            pickup_day INT NOT NULL,
                                                                            pickup_hour INT NOT NULL,
                                                                            pickup_minute INT NOT NULL,
                                                                            pickup_day_of_week INT NOT NULL,
                                                                            pickup_day_name VARCHAR(15) NOT NULL,
                                                                            pickup_is_month_end BOOLEAN NOT NULL,
                                                                            pickup_is_year_end BOOLEAN NOT NULL,
                                                                            dropoff_year INT NOT NULL,
                                                                            dropoff_month INT NOT NULL,
                                                                            dropoff_day INT NOT NULL,
                                                                            dropoff_hour INT NOT NULL,
                                                                            dropoff_minute INT NOT NULL,
                                                                            dropoff_day_of_week INT NOT NULL,
                                                                            dropoff_day_name VARCHAR(15) NOT NULL,
                                                                            dropoff_is_month_end BOOLEAN NOT NULL,
                                                                            dropoff_is_year_end BOOLEAN NOT NULL)
                                                                            """


def fact_dimension_sql_statement(datetim_table_name, dimension_table_name, fact_table_name):

    create_time_table_statement = f"""CREATE TABLE IF NOT EXISTS {datetim_table_name} (datetime_id VARCHAR PRIMARY KEY, 
                                                                           tpep_dropoff_datetime TIMESTAMP,
                                                                           tpep_pickup_datetime TIMESTAMP,
                                                                           pickup_year INT NOT NULL,
                                                                            pickup_month INT NOT NULL,
                                                                            pickup_day INT NOT NULL,
                                                                            pickup_hour INT NOT NULL,
                                                                            pickup_minute INT NOT NULL,
                                                                            pickup_day_of_week INT NOT NULL,
                                                                            pickup_day_name VARCHAR(15) NOT NULL,
                                                                            pickup_is_month_end BOOLEAN NOT NULL,
                                                                            pickup_is_year_end BOOLEAN NOT NULL,
                                                                            dropoff_year INT NOT NULL,
                                                                            dropoff_month INT NOT NULL,
                                                                            dropoff_day INT NOT NULL,
                                                                            dropoff_hour INT NOT NULL,
                                                                            dropoff_minute INT NOT NULL,
                                                                            dropoff_day_of_week INT NOT NULL,
                                                                            dropoff_day_name VARCHAR(15) NOT NULL,
                                                                            dropoff_is_month_end BOOLEAN NOT NULL,
                                                                            dropoff_is_year_end BOOLEAN NOT NULL)
                                                                            """

    create_dimension_table_statement = f"""CREATE TABLE IF NOT EXISTS {dimension_table_name} (datetime_id VARCHAR, 
                                                                                     RATECODEID FLOAT,
                                                                                     STORE_AND_FWD_FLAG VARCHAR(2),
                                                                                     PULOCATIONID INT,
                                                                                     DOLOCATIONID INT,
                                                                                     PAYMENT_TYPE INT ,
                                                                                     FOREIGN KEY(datetime_id)
                                                                                     REFERENCES {datetim_table_name} (datetime_id));"""

    fact_table_creation_sql = f"""CREATE TABLE IF NOT EXISTS {fact_table_name} (datetime_id VARCHAR, 
                                                                    airport_fee FLOAT,
                                                                    congestion_surcharge FLOAT,
                                                                    total_amount FLOAT,
                                                                        improvement_surcharge FLOAT,
                                                                        tolls_amount FLOAT,
                                                                        tip_amount FLOAT,
                                                                        mta_tax FLOAT,
                                                                        extra FLOAT,
                                                                        fare_amount FLOAT,
                                                                        TRIP_DISTANCE FLOAT,
                                                                        PASSENGER_COUNT FLOAT,
                                                                        FOREIGN KEY(datetime_id)
                                                                        REFERENCES {datetim_table_name} (datetime_id));"""
    
    return  create_time_table_statement,create_dimension_table_statement, fact_table_creation_sql

def create_table_load_data(df:pd.DataFrame, table_name, sql_statement,test=False):
    drop_statement = f'DROP TABLE IF EXISTS {table_name} cascade'
    if test:
        with engine.connect() as con:
            con.exec_driver_sql(drop_statement)
    
    with engine.connect() as con:
        con.exec_driver_sql(sql_statement)

    df_column_lists = list(df.columns)

    table_value_list = df.apply(row_to_tuple, column=df_column_lists, axis=1).tolist()

    insert_statement_table = f"""INSERT INTO {table_name} ({", ".join(df_column_lists)}) VALUES({", ".join(['%s'] * len(df_column_lists))})"""

    print(f'about to insert into {table_name}')

    with engine.connect() as con:
        con.execute(insert_statement_table, table_value_list)

    print(f'done inserting into {table_name}')

    


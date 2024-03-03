# from ny_taxi_transform import df_time_table_dropoff, fact_data_table, dimension_table_data
import pyspark
import pandas as pd
from pyspark.sql import functions as F

# try:
#     engine.connect()
# except:
#     # engine = create_engine('postgresql+psycopg2://idowu_user:idowupassword@postgres_service_new:5432')
#     # con = engine.connect()
#     # con.execute("commit")
#     # con.exec_driver_sql("create database nyc_taxi_database")
#     # con.close()
#     pass

# engine = create_engine('postgresql+psycopg2://idowu_user:idowupassword@postgres_service_new:5432/nyc_taxi')
# def row_to_tuple(row, column):
#     # values = [ df_time_table_dropoff[col][i] for col in ordered_column_time]
#     values = [str(row[col]) if 'tpep' in col else row[col] for col in column]
#     return tuple(values)

# create_time_table_statement = """CREATE TABLE IF NOT EXISTS raw.datetime_trip_table (datetime_id VARCHAR PRIMARY KEY, 
#                                                                            tpep_dropoff_datetime TIMESTAMP,
#                                                                            tpep_pickup_datetime TIMESTAMP,
#                                                                            pickup_year INT NOT NULL,
#                                                                             pickup_month INT NOT NULL,
#                                                                             pickup_day INT NOT NULL,
#                                                                             pickup_hour INT NOT NULL,
#                                                                             pickup_minute INT NOT NULL,
#                                                                             pickup_day_of_week INT NOT NULL,
#                                                                             pickup_day_name VARCHAR(15) NOT NULL,
#                                                                             pickup_month_name VARCHAR(15) NOT NULL,
#                                                                             pickup_is_month_end BOOLEAN NOT NULL,
#                                                                             dropoff_year INT NOT NULL,
#                                                                             dropoff_month INT NOT NULL,
#                                                                             dropoff_day INT NOT NULL,
#                                                                             dropoff_hour INT NOT NULL,
#                                                                             dropoff_minute INT NOT NULL,
#                                                                             dropoff_day_of_week INT NOT NULL,
#                                                                             dropoff_day_name VARCHAR(15) NOT NULL,
#                                                                             droppoff_month_name VARCHAR(15) NOT NULL,
#                                                                             dropoff_is_month_end BOOLEAN NOT NULL
#                                                                             """


# def fact_dimension_sql_statement(datetime_table_name, dimension_table_name, fact_table_name, db_user, drop_table=False):
# # CREATE SCHEMA schema_name AUTHORIZATION user_name
#     if drop_table:
#         create_time_table_statement = f"""CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION {db_user};
#         DROP TABLE IF EXISTS raw.{datetime_table_name} cascade;
#         CREATE TABLE IF NOT EXISTS raw.{datetime_table_name} (datetime_id VARCHAR PRIMARY KEY, 
#                                                                             tpep_dropoff_datetime TIMESTAMP,
#                                                                             tpep_pickup_datetime TIMESTAMP,
#                                                                             pickup_year INT NOT NULL,
#                                                                                 pickup_month INT NOT NULL,
#                                                                                 pickup_day INT NOT NULL,
#                                                                                 pickup_hour INT NOT NULL,
#                                                                                 pickup_minute INT NOT NULL,
#                                                                                 pickup_day_of_week INT NOT NULL,
#                                                                                 pickup_day_name VARCHAR(15) NOT NULL,
#                                                                                 pickup_is_month_end BOOLEAN NOT NULL,
#                                                                                 dropoff_year INT NOT NULL,
#                                                                                 dropoff_month INT NOT NULL,
#                                                                                 dropoff_day INT NOT NULL,
#                                                                                 dropoff_hour INT NOT NULL,
#                                                                                 dropoff_minute INT NOT NULL,
#                                                                                 dropoff_day_of_week INT NOT NULL,
#                                                                                 dropoff_day_name VARCHAR(15) NOT NULL,
#                                                                                 dropoff_is_month_end BOOLEAN NOT NULL
#                                                                                 )
#                                                                                 """

#         create_dimension_table_statement = f"""CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION {db_user};
#         DROP TABLE IF EXISTS raw.{dimension_table_name} cascade;
#         CREATE TABLE IF NOT EXISTS raw.{dimension_table_name} (datetime_id VARCHAR, 
#                                                                                         RATECODEID FLOAT,
#                                                                                         STORE_AND_FWD_FLAG VARCHAR(2),
#                                                                                         PULOCATIONID INT,
#                                                                                         DOLOCATIONID INT,
#                                                                                         PAYMENT_TYPE INT
#                                                                                         );"""

#         fact_table_creation_sql = f"""CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION {db_user};
#         DROP TABLE IF EXISTS raw.{fact_table_name} cascade;
#         CREATE TABLE IF NOT EXISTS raw.{fact_table_name} (datetime_id VARCHAR, 
#                                                                         airport_fee FLOAT,
#                                                                         congestion_surcharge FLOAT,
#                                                                         total_amount FLOAT,
#                                                                             improvement_surcharge FLOAT,
#                                                                             tolls_amount FLOAT,
#                                                                             tip_amount FLOAT,
#                                                                             mta_tax FLOAT,
#                                                                             extra FLOAT,
#                                                                             fare_amount FLOAT,
#                                                                             TRIP_DISTANCE FLOAT,
#                                                                             PASSENGER_COUNT FLOAT
#                                                                             );"""
#         return  create_time_table_statement,create_dimension_table_statement, fact_table_creation_sql
#     else:
#         create_time_table_statement = f"""CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION {db_user};
#         CREATE TABLE IF NOT EXISTS raw.{datetime_table_name} (datetime_id VARCHAR PRIMARY KEY, 
#                                                                             tpep_dropoff_datetime TIMESTAMP,
#                                                                             tpep_pickup_datetime TIMESTAMP,
#                                                                             pickup_year INT NOT NULL,
#                                                                                 pickup_month INT NOT NULL,
#                                                                                 pickup_day INT NOT NULL,
#                                                                                 pickup_hour INT NOT NULL,
#                                                                                 pickup_minute INT NOT NULL,
#                                                                                 pickup_day_of_week INT NOT NULL,
#                                                                                 pickup_day_name VARCHAR(15) NOT NULL,
#                                                                                 pickup_is_month_end BOOLEAN NOT NULL,
#                                                                                 dropoff_year INT NOT NULL,
#                                                                                 dropoff_month INT NOT NULL,
#                                                                                 dropoff_day INT NOT NULL,
#                                                                                 dropoff_hour INT NOT NULL,
#                                                                                 dropoff_minute INT NOT NULL,
#                                                                                 dropoff_day_of_week INT NOT NULL,
#                                                                                 dropoff_day_name VARCHAR(15) NOT NULL,
#                                                                                 dropoff_is_month_end BOOLEAN NOT NULL
#                                                                                 )
#                                                                                 """
#         create_dimension_table_statement = f"""CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION {db_user};
#         CREATE TABLE IF NOT EXISTS raw.{dimension_table_name} (datetime_id VARCHAR, 
#                                                                                         RATECODEID FLOAT,
#                                                                                         STORE_AND_FWD_FLAG VARCHAR(2),
#                                                                                         PULOCATIONID INT,
#                                                                                         DOLOCATIONID INT,
#                                                                                         PAYMENT_TYPE INT
#                                                                                         );"""

#         fact_table_creation_sql = f"""CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION {db_user};
#         CREATE TABLE IF NOT EXISTS raw.{fact_table_name} (datetime_id VARCHAR, 
#                                                                         airport_fee FLOAT,
#                                                                         congestion_surcharge FLOAT,
#                                                                         total_amount FLOAT,
#                                                                             improvement_surcharge FLOAT,
#                                                                             tolls_amount FLOAT,
#                                                                             tip_amount FLOAT,
#                                                                             mta_tax FLOAT,
#                                                                             extra FLOAT,
#                                                                             fare_amount FLOAT,
#                                                                             TRIP_DISTANCE FLOAT,
#                                                                             PASSENGER_COUNT FLOAT
#                                                                             );"""

    
#         return  create_time_table_statement,create_dimension_table_statement, fact_table_creation_sql


def fact_dimension_sql_statement(datetime_table_name, dimension_table_name, fact_table_name, db_user,schema_name, drop_table=False):
# CREATE SCHEMA schema_name AUTHORIZATION user_name
  if drop_table:
    create_time_table_statement = f"""CREATE SCHEMA IF NOT EXISTS {schema_name} AUTHORIZATION {db_user};
    DROP TABLE IF EXISTS {schema_name}.{datetime_table_name} cascade;
    CREATE TABLE IF NOT EXISTS {schema_name}.{datetime_table_name} (datetime_id VARCHAR PRIMARY KEY, 
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
                                                                                dropoff_year INT NOT NULL,
                                                                                dropoff_month INT NOT NULL,
                                                                                dropoff_day INT NOT NULL,
                                                                                dropoff_hour INT NOT NULL,
                                                                                dropoff_minute INT NOT NULL,
                                                                                dropoff_day_of_week INT NOT NULL,
                                                                                dropoff_day_name VARCHAR(15) NOT NULL,
                                                                                dropoff_is_month_end BOOLEAN NOT NULL
                                                                                )
                                                                                """

    create_dimension_table_statement = f"""CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION {db_user};
    DROP TABLE IF EXISTS {schema_name}.{dimension_table_name} cascade;
    CREATE TABLE IF NOT EXISTS {schema_name}.{dimension_table_name} (datetime_id VARCHAR, 
                                                                                        RATECODEID FLOAT,
                                                                                        STORE_AND_FWD_FLAG VARCHAR(2),
                                                                                        PULOCATIONID INT,
                                                                                        DOLOCATIONID INT,
                                                                                        PAYMENT_TYPE INT
                                                                                        );"""

    fact_table_creation_sql = f"""CREATE SCHEMA IF NOT EXISTS {schema_name} AUTHORIZATION {db_user};
    DROP TABLE IF EXISTS {schema_name}.{fact_table_name} cascade;
    CREATE TABLE IF NOT EXISTS {schema_name}.{fact_table_name} (datetime_id VARCHAR, 
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
                                                                            PASSENGER_COUNT FLOAT
                                                                            );"""
    return  create_time_table_statement,create_dimension_table_statement, fact_table_creation_sql
  else:
    create_time_table_statement = f"""CREATE SCHEMA IF NOT EXISTS {schema_name} AUTHORIZATION {db_user};
    CREATE TABLE IF NOT EXISTS {schema_name}.{datetime_table_name} (datetime_id VARCHAR PRIMARY KEY, 
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
                                                                                dropoff_year INT NOT NULL,
                                                                                dropoff_month INT NOT NULL,
                                                                                dropoff_day INT NOT NULL,
                                                                                dropoff_hour INT NOT NULL,
                                                                                dropoff_minute INT NOT NULL,
                                                                                dropoff_day_of_week INT NOT NULL,
                                                                                dropoff_day_name VARCHAR(15) NOT NULL,
                                                                                dropoff_is_month_end BOOLEAN NOT NULL
                                                                                )
                                                                                """
    create_dimension_table_statement = f"""CREATE SCHEMA IF NOT EXISTS {schema_name} AUTHORIZATION {db_user};
    CREATE TABLE IF NOT EXISTS {schema_name}.{dimension_table_name} (datetime_id VARCHAR, 
                                                                                        RATECODEID FLOAT,
                                                                                        STORE_AND_FWD_FLAG VARCHAR(2),
                                                                                        PULOCATIONID INT,
                                                                                        DOLOCATIONID INT,
                                                                                        PAYMENT_TYPE INT
                                                                                        );"""

    fact_table_creation_sql = f"""CREATE SCHEMA IF NOT EXISTS {schema_name} AUTHORIZATION {db_user};
    CREATE TABLE IF NOT EXISTS {schema_name}.{fact_table_name} (datetime_id VARCHAR, 
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
                                                                            PASSENGER_COUNT FLOAT
                                                                            );"""

    
    return  create_time_table_statement,create_dimension_table_statement, fact_table_creation_sql
def read_load_data_db(df, table_name, schema_name, jdbc_uri, db_user, db_password_quote, engine):
    sql_query_check_data = f"""SELECT * FROM {schema_name}.{table_name} LIMIT 10"""
    try:
        print(f'about to read the previous data from the {schema_name}.{table_name}')
        previous_record = pd.read_sql(sql_query_check_data, con=engine)
        print(previous_record.shape)
        datetime_id_df_list = list(previous_record['datetime_id'])
        records_in_py_df = df.filter(F.col("datetime_id").isin(datetime_id_df_list)).count()
        print('got the previous records')
        if previous_record.shape[0] > 3 and records_in_py_df == 0:
            print(f"appending to table {table_name} with {schema_name}")
            df.repartition(100).write.format("jdbc").option("url", jdbc_uri).option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user",db_user).option("password", db_password_quote).mode('append').save()
        elif previous_record.shape[0] > 3 and records_in_py_df > 0:
            print(f"cannot append duplicate values to table {table_name} with {schema_name}")
        elif previous_record.shape[0] == 0 and records_in_py_df == 0:
            df.repartition(100).write.format("jdbc").option("url", jdbc_uri).option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user",db_user).option("password", db_password_quote).mode('append').save()
    except:
        print('could not read previous records')
        print('there are no records in the database')
        df.repartition(100).write.format("jdbc").option("url", jdbc_uri).option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user",db_user).option("password", db_password_quote).mode('append').save()
        print('done overrwriting the data into the table')
    # datetime_id_df_list = list(previous_record['datetime_id'])
    # print('inserting into the recordds ')
    # records_in_py_df = df.filter(F.col("datetime_id").isin(datetime_id_df_list)).count()
    # # df.repartition(100).write.format("jdbc").option("url", jdbc_uri).option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user",db_user).option("password", db_password_quote).mode('append').save()
    # if previous_record.shape[0] > 3 and records_in_py_df == 0:
    #         print(f"appending to table {table_name} with {schema_name}")
    #         df.repartition(100).write.format("jdbc").option("url", jdbc_uri).option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user",db_user).option("password", db_password_quote).mode('append').save()
    # try:
    #     previous_record = pd.read_sql(sql_query_check_data, con=engine)
    #     datetime_id_df_list = list(previous_record['datetime_id'])
    #     records_in_py_df = df.filter(F.col("datetime_id").isin(datetime_id_df_list)).count()
    #     if previous_record.shape[0] > 3 and records_in_py_df == 0:
    #         print(f"appending to table {table_name} with {schema_name}")
    #         df.repartition(100).write.format("jdbc").option("url", jdbc_uri).option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user",db_user).option("password", db_password_quote).mode('append').save()
    #     elif previous_record.shape[0] > 3 and records_in_py_df > 0:
    #         print(f"cannot append duplicate values to table {table_name} with {schema_name}")
    #         # df.repartition(100).write.format("jdbc").option("url", "jdbc:postgresql://localhost:5433/ny_taxi_database").option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user","idowuuser").option("password", "passwordguddy").mode('append').save()
    # except:
    #     print('there are no new records in the database')
    #     df.repartition(100).write.format("jdbc").option("url", jdbc_uri).option("driver", "org.postgresql.Driver").option("dbtable", f"{schema_name}.{table_name}").option("user",db_user).option("password", db_password_quote).mode('append').save()
def create_table_load_data(df:pyspark.sql.DataFrame, table_name:str, schema_name:str, sql_statement:str,engine,jdbc_uri, db_user, db_password_quote,test: bool=False)->None:
    print("running the create_table_load_data")
    drop_statement = f'DROP TABLE IF EXISTS {schema_name}.{table_name} cascade'
    if test:
        with engine.begin() as con:
            con.exec_driver_sql(drop_statement)
            # con.commit()
    
    with engine.begin() as con:
        # if 'raw' not in con.dialect.get_schema_names(con):
            
        con.exec_driver_sql(sql_statement)
        print(f"created the table and schema {table_name} and {schema_name}")
        # con.commit()

    # df_column_lists = list(df.columns)

    # table_value_list = df.apply(row_to_tuple, column=df_column_lists, axis=1).tolist()

    # insert_statement_table = f"""INSERT INTO {table_name} ({", ".join(df_column_lists)}) VALUES({", ".join(['%s'] * len(df_column_lists))})"""

    # print(f'about to insert into {table_name}')

    # with engine.connect() as con:
    #     con.execute(insert_statement_table, table_value_list)

    # print(f'done inserting into {table_name}')
    print('about to execute read_load_data_db')
    read_load_data_db(df,table_name, schema_name, jdbc_uri, db_user, db_password_quote, engine)

    


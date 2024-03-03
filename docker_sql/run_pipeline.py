# import sys
# sys.path.insert(0,'/home/idowuileks/Desktop/tech_project/nyc_taxi_project/docker_sql')

import sys

# Verify the path you are adding to sys.path is correct
sys.path.insert(0, '/home/idowuileks/Desktop/tech_project/nyc_taxi_project/docker_sql')
import etl_script 
from etl_script import ny_run_etl_pipeline
# import download_load_data
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
# from airflow.models import DAG
# from airflow.operators.python_operator import PythonOperator
import os
from pyspark.sql.session import SparkSession
from sqlalchemy import create_engine,sql, schema
from urllib.parse import quote_plus
import pandas as pd
import pyspark
from pyspark.sql import functions as F
# engine = create_engine('postgresql+psycopg2://idowuadmin:EKabagoodness@96@idowupostgresserver.postgres.database.azure.com:5432/ny_taxi_database')
# db_host = "idowupostgresserver.postgres.database.azure.com"
# db_user = "idowuadmin"
# db_password = "Ekabagoodness@96"
# db_password_quote = quote_plus(db_password)
# db_name = "ny_taxi_database"
# db_port = 5432
db_host = "localhost"
db_user = "idowuuser"
db_password = "passwordguddy"
db_password_quote = quote_plus(db_password)
db_name = "ny_taxi_database"
db_port = 5433
ngrok_url = 'https://ebc7-2a01-11-8030-1670-b435-cfdc-3b2e-3363.ngrok-free.app'
database_uri = 'postgresql+psycopg2://' + db_user + ':' + db_password_quote + '@' + ngrok_url + '/' + db_name
jdbc_uri =f"jdbc:postgresql://{db_host}:{str(db_port)}/{db_name}"

engine = create_engine(database_uri)
cwd = os.getcwd()
spark = SparkSession.builder.appName("hello").config("spark.master", "local[*]").config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2020, 1, 1),
#     'end_date': datetime(2020, 1, 2),
#     'email': [''],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'run_pipeline', default_args = default_args)



# with dag as dag:
#     t1 = PythonOperator(
#         task_id='download_load_data',
#         python_callable=ny_run_etl_pipeline.download_load_data,
#         op_kwargs={'year': 2020, 'month': 3, 'parent_folder_path': cwd,'spark':spark,'db_user': db_user,'jdbc_uri': jdbc_uri,'db_password_quote': db_password_quote, 'engine': engine},
#         dag=dag
#     )

#     t1

op_kwargs={'year': 2020, 'month': 3, 'parent_folder_path': cwd,'spark':spark,'db_user': db_user,'jdbc_uri': jdbc_uri,'db_password_quote': db_password_quote, 'engine': engine}
ny_run_etl_pipeline.download_load_data(**op_kwargs)
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
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
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

db_host = os.getenv('POSTGRES_HOST', 'localhost')
db_user = os.getenv('POSTGRES_USER', 'idowuuser')
db_password = os.getenv('POSTGRES_PASSWORD')
db_name = os.getenv('POSTGRES_DBNAME', 'ny_taxi_database')
db_port = os.getenv('POSTGRES_PORT')
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
cwd = os.getcwd()
spark = SparkSession.builder.appName("hello").config("spark.master", "local[*]").config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 1),
    'end_date': datetime(2021, 12, 31),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    # 'schedule': '@once',
    'retry_delay': timedelta(minutes=5),
    # 'catchup':  False
}
year = '{{ execution_date.strftime(\'%Y\') }}'
month = '{{ execution_date.strftime(\'%m\') }}'
dag = DAG(
    'run_pipeline_new',schedule_interval = "@monthly" ,catchup=True,default_args = default_args)

# date = "{{ ds }}"
# date_split = str(date).split('-')[0] + '-' + str(date.split('-'))
# year, month = date_split[0], date_split[1]
# year = 2019
# month = 1
# def get_folder_path(ti):
#     date = '{{execution_date.strftime("%Y-%m")}}'
#     date_split = date.split('-')

#     year, month = date_split[0], date_split[1]

#     ti.xcom_push(key='year', value=year)
#     ti.xcom_push(key='month', value=month)

#     return year, month

# def process_execution_date(execution_date):
#     # Extract year from execution_date
#     year_ex = execution_date
    
    
#     # Now you can use the 'year' variable as needed
#     print(f'Year extracted from execution_date: {year_ex.split()}')

#     return year_ex

# Example usage
# execution_date = "{{ ds }}"
# year_ex = process_execution_date(execution_date)


# print(year, month)

# print(date)
# print(date_split)

def remove_file(ti):
    data_path = ti.xcom_pull(key='data_path', task_ids='download_load_data')
    # os.remove(data_path)
    # os.system(f'rm -rf {data_path}')
    folder_path_file = ti.xcom_pull(key='folder_path', task_ids='download_load_data')

    os.system(f'rm -rf {folder_path_file}')

    os.system('rm -rf $AIRFLOW_HOME/wget-log*')

# URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data' 
# URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
# DATE_FORMAT= '{{ execution_date.strftime(\'%Y-%m\') }}'
# DATE_FORMAT_BETTER = DATE_FORMAT.replace('_','')
# '{{ execution_date.strftime(\'%Y\') }}'
# DATE_FORMAT_SPLIT = DATE_FORMAT_BETTER.split('-')
# year = DATE_FORMAT_SPLIT[0]
def run_dbt_command(dbt_path:str):
    os.chdir(dbt_path)
    os.system('dbt build')
    print('done building the dbt model')
    os.chdir(cwd)

def run_echo(year):
    os.system(f'echo {year}')
    # print(year)
    # print(year.split('-'))
    print(type(year))
    # print(year.split('-'))

with dag as dag:
    # t1 = PythonOperator(
    #     task_id = 'run_echo',
    #     python_callable = run_echo,
    #     op_kwargs={"year": '{{ execution_date.strftime(\'%Y\') }}'},
    #     dag = dag
    # )

    # t1
    # t1 = BashOperator(
    #     task_id = 'export_execution_date',
    #     bash_command = f"echo {year}",
    #     dag = dag, 
    #     env = {'Execution_date': date}
    # )
    # t1 = BashOperator(
    #     task_id = 'echo_date_env',
    #     bash_command = 'echo $Execution_date',
    #     dag = dag
    # )
    t1 = PythonOperator(
        task_id='download_load_data',
        python_callable=ny_run_etl_pipeline.download_load_data,
        op_kwargs={'year': year, 'month': month , 'parent_folder_path': '/home/idowuileks/Desktop/tech_project/nyc_taxi_project/docker_sql/etl_script/data','spark':spark,'db_user': db_user,'jdbc_uri': jdbc_uri,'db_password_quote': db_password_quote,'engine': engine,'drop_table': False}
    )

    

    t2 = PythonOperator(
        task_id = 'remove_file',
        python_callable = remove_file
    )

    t3 = PythonOperator(
        task_id = 'run_dbt_command',
        python_callable = run_dbt_command,
        op_kwargs={"dbt_path": '/home/idowuileks/Desktop/tech_project/nyc_taxi_project/docker_sql/ny_taxi_test'}
    )
    t1 >> t2 >> t3
    
   
# op_kwargs={'year': 2020, 'month': 3, 'parent_folder_path': cwd,'spark':spark,'db_user': db_user,'jdbc_uri': jdbc_uri,'db_password_quote': db_password_quote, 'engine': engine}
# ny_run_etl_pipeline.download_load_data(**op_kwargs)
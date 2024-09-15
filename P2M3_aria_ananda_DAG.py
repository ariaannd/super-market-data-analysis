'''
=================================================
Milestone 3

Name  : Aria Ananda
Batch : FTDS-013-HCK 

This program is designed to automate the process of extracting data from PostgreSQL, performing data cleaning, 
and then transferring the cleaned data to ElasticSearch.
=================================================

'''

# Import Libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from elasticsearch import Elasticsearch
import pandas as pd

# Connecting to Postgres
from sqlalchemy import create_engine 

def load_data_to_pg():
    '''
    This function is used to load csv data to postgress database

        Params:
            - database (str): the name of the local database
            - username (str): the name of username used to access the local database
            - password (str): the password used to access the local database

        Return: df table loaded to postgres 

        Example of use: load_data_to_postgres('database', 'username', 'password')
    '''
    # Define database, username, password, and host
    database = "aria_milestone3"
    username = "aria_milestone3"
    password = "aria_milestone3"
    host = "postgres"

    # Define URL connecting to PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use URL connecting to SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Fetch data using SQLAlchemy
    df = pd.read_csv('/opt/airflow/dags/P2M3_aria_ananda_data_raw.csv')
    df.to_sql('table_m3', conn, index=False, if_exists='replace')
    
    
def fetch_data_pg():
    '''
    This function is used to fetch data from PostgreSQL
    
    Params:
            - database (str): the name of the local database
            - username (str): the name of username used to access the local database
            - password (str): the password used to access the local database

        Return: df table fetched from postgres 

        Example of use: fetch_data_pg('database', 'username', 'password')
    '''
    # Define database, username, password, and host
    database = "aria_milestone3"
    username = "aria_milestone3"
    password = "aria_milestone3"
    host = "postgres"

    # Make URL Connecting PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use URL Connecting to SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Fetch data using SQLAlchemy
    df = pd.read_sql_query("select * from table_m3", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/P2M3_aria_ananda_data_new.csv', sep=',', index=False)
    

def preprocessing(): 
    ''' 
    This function is used to preprocess and clean the dataset
    
        Params: -

        Return: -

        Example of use: preprocessing()
    '''
    # Define the Data
    data = pd.read_csv("/opt/airflow/dags/P2M3_aria_ananda_data_new.csv")

    # Data Cleaning and Preprocessing
    data['Date'] = pd.to_datetime(data['Date'])
    data['Time'] = pd.to_datetime(data['Time'], format='%H:%M')
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.strip()
    data.columns = data.columns.str.replace(' ', '_')
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data.to_csv('/opt/airflow/dags/P2M3_aria_ananda_data_clean.csv', index=False)

def upload_to_elasticsearch():
    '''
    This function is used to upload preprocessed data to elastisearch
        
        Params: -

        Return: -

        Example of use: upload_to_elasticsearch()
    '''
    
    # Define elastisearch url and data
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_aria_ananda_data_clean.csv')
    
    # Iterate DataFrame converts it to a dictionary representation, indexes it into Elasticsearch with an incremented ID, and prints the response from Elasticsearch.
    for i, r in df.iterrows():
        doc = r.to_dict()
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
# Define Airflow DAG with tasks for loading data into PostgreSQL, fetching data from PostgreSQL, preprocessing data, and uploading data to Elasticsearch, scheduled daily at 06:30      
default_args = {
    'owner': 'Aria Ananda', 
    'start_date': datetime(2023, 12, 24, 12, 00)
}

with DAG(
    "P2M3_aria_ananda_DAG", # Project Name
    description='Milestone_3',
    schedule_interval='30 6 * * *', # Scheduled 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    # Task : 1
    load_data_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_pg)
    
    # task: 2
    fetch_data_from_postgres = PythonOperator(
        task_id='fetch_data_from_postgres',
        python_callable=fetch_data_pg) 

    # Task: 3
    data_preprocessing = PythonOperator(
        task_id='data_preprocessing',
        python_callable=preprocessing)

    # Task: 4
    upload_data_to_elasticsreatch = PythonOperator(
        task_id='upload_data_to_elasticsearch',
        python_callable=upload_to_elasticsearch)

    # Airflow Process
    load_data_to_postgres >> fetch_data_from_postgres >> data_preprocessing >> upload_data_to_elasticsreatch





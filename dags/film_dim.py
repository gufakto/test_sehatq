from airflow import DAG
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.mysql_hook import MySqlHook
from base.utils import table_to_csv, csv_to_df, csv_to_table
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

 
silver_film = "/opt/airflow/dags/silvers/film.csv"
film_bronze = "/opt/airflow/dags/bronze/film.csv"

film_dim = "film_dim"

with DAG(dag_id="film_dim", schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Jakarta"), catchup=False, tags=["sehatq"]) as dag:
    
    def extract(**kwargs):
        film_query = "SELECT film_id, title, release_year, rating FROM `film`"
        table_to_csv(film_query, film_bronze)

    def transform(**kwargs):
        df_film = csv_to_df(film_bronze)
        df_film.to_csv(silver_film, index=False)  

    def load(**kwargs):
        csv_to_table(film_dim, silver_film)


    extract_task = PythonOperator(task_id='extract',python_callable=extract,)
    transform_task = PythonOperator(task_id='transform', python_callable=transform,)
    load_task = PythonOperator(task_id='load', python_callable=load,)
    extract_task >> transform_task >> load_task

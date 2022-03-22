from airflow import DAG
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.mysql_hook import MySqlHook
from base.utils import table_to_csv, csv_to_df, csv_to_table
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

 
silver_address = "/opt/airflow/dags/silvers/address.csv"
store_silver = "/opt/airflow/dags/silvers/store.csv"
store_bronze = "/opt/airflow/dags/bronze/store.csv"

store_dim = "store_dim"

with DAG(dag_id="store_dim", schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Jakarta"), catchup=False, tags=["sehatq"]) as dag:
    
    def extract(**kwargs):
        store_query = "SELECT store_id, manager_staff_id, address_id FROM `store`"
        table_to_csv(store_query, store_bronze)

    def transform(**kwargs):
        df_store = csv_to_df(store_bronze)
        df_addr = csv_to_df(silver_address)
        
        df_ = pd.merge(
                    df_store,
                    df_addr, 
                    on=['address_id'], 
                    how="left"
                )
        df_['store'] = df_[['city', 'country']].agg(','.join, axis=1)
        df_=df_[['store_id', 'manager_staff_id', 'store']]
        df_.to_csv(store_silver, index=False)  

    def load(**kwargs):
        csv_to_table(store_dim, store_silver)


    extract_task = PythonOperator(task_id='extract',python_callable=extract,)
    transform_task = PythonOperator(task_id='transform', python_callable=transform,)
    load_task = PythonOperator(task_id='load', python_callable=load,)
    extract_task >> transform_task >> load_task

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
silver_staff = "/opt/airflow/dags/silvers/staff.csv"
staff_bronze = "/opt/airflow/dags/bronze/staff.csv"

staff_dim = "staff_dim"

with DAG(dag_id="staff_dim", schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Jakarta"), catchup=False, tags=["sehatq"]) as dag:
    
    def extract(**kwargs):
        staff_query = "SELECT staff_id, first_name, last_name, address_id, email, store_id, active, username FROM `staff`"
        table_to_csv(staff_query, staff_bronze)

    def transform(**kwargs):
        df_staff = csv_to_df(staff_bronze)
        df_addr = csv_to_df(silver_address)
        
        df_ = pd.merge(
                    df_staff,
                    df_addr, 
                    on=['address_id'], 
                    how="left"
                )
        df_.to_csv(silver_staff, index=False)  

    def load(**kwargs):
        csv_to_table(staff_dim, silver_staff)


    extract_task = PythonOperator(task_id='extract',python_callable=extract,)
    transform_task = PythonOperator(task_id='transform', python_callable=transform,)
    load_task = PythonOperator(task_id='load', python_callable=load,)
    extract_task >> transform_task >> load_task

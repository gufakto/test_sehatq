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
cust_silver = "/opt/airflow/dags/silvers/customer.csv"
customer_bronze = "/opt/airflow/dags/bronze/customer.csv"

customer_dim = "customer_dim"

with DAG(dag_id="customer_dim", schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Jakarta"), catchup=False, tags=["sehatq"]) as dag:
    
    def extract(**kwargs):
        cust_query = "SELECT * FROM `customer`"
        table_to_csv(cust_query, customer_bronze)

    def transform(**kwargs):
        df_cust = csv_to_df(customer_bronze)
        df_addr = csv_to_df(silver_address)
        
        df_ = pd.merge(
                    df_cust,
                    df_addr, 
                    on=['address_id'], 
                    how="left"
                )
        df_=df_[['customer_id', 'store_id', 'first_name', 'last_name', 'email', 'active', 'create_date', 'district',  'city', 'country']]
        df_.to_csv(cust_silver, index=False)  

    def load(**kwargs):
        csv_to_table(customer_dim, cust_silver)


    extract_task = PythonOperator(task_id='extract',python_callable=extract,)
    transform_task = PythonOperator(task_id='transform', python_callable=transform,)
    load_task = PythonOperator(task_id='load', python_callable=load,)
    extract_task >> transform_task >> load_task

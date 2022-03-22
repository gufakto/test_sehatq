from airflow import DAG
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.mysql_hook import MySqlHook
from base.utils import table_to_csv, csv_to_df, csv_to_table
import pandas as pd
from airflow.operators.python import PythonOperator

address_bronze = "/opt/airflow/dags/bronze/address.csv"
city_bronze = "/opt/airflow/dags/bronze/city.csv"
country_bronze = "/opt/airflow/dags/bronze/country.csv"

address_silver = "/opt/airflow/dags/silvers/address.csv"

with DAG(dag_id="adress_silver", schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Jakarta"), catchup=False, tags=["sehatq"]) as dag:
    def extract(**kwargs):
        addr_query = "SELECT address_id, district, city_id FROM `address`"
        city_query = "SELECT city_id, city, country_id FROM `city`"
        coun_query = "SELECT country_id, country FROM `country`"
        
        table_to_csv(addr_query, address_bronze)
        table_to_csv(city_query, city_bronze)
        table_to_csv(coun_query, country_bronze)

    def transform(**kwargs):
        df_addr = csv_to_df(address_bronze)
        df_city = csv_to_df(city_bronze)
        df_ctr = csv_to_df(country_bronze)
        
        df_ = pd.merge(
                pd.merge(
                    df_addr,
                    df_city, 
                    on=['city_id'], 
                    how="left"
                ),
                df_ctr,
                on=["country_id"],
                how="left"
            )
        df_ = df_[['address_id', 'district', 'city', 'country']]
        df_.to_csv(address_silver, index=False)

    extract_task = PythonOperator(task_id='extract',python_callable=extract,)
    transform_task = PythonOperator(task_id='transform', python_callable=transform,)
    extract_task >> transform_task 

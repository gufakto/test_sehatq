from airflow import DAG
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.mysql_hook import MySqlHook
from base.utils import table_to_csv, csv_to_df, csv_to_table
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

 
inv_bronze = "/opt/airflow/dags/bronze/inventory.csv"
rent_bronze = "/opt/airflow/dags/bronze/rental.csv"
paym_bronze = "/opt/airflow/dags/bronze/payment.csv"

inv_silver = "/opt/airflow/dags/silvers/inventory.csv"
rent_silver = "/opt/airflow/dags/silvers/rental.csv"
paym_silver = "/opt/airflow/dags/silvers/payment.csv"
sales_silver = "/opt/airflow/dags/silvers/sales.csv"
sales_date_silver = "/opt/airflow/dags/silvers/sales_date.csv"

sales_fact = "sales_fact"
sales_withdate_fact = "sales_withdate_fact"

with DAG(dag_id="sales_fact", schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Jakarta"), catchup=False, tags=["sehatq"]) as dag:
    
    def extract(**kwargs):
        inv_query = "SELECT inventory_id, film_id, store_id FROM `inventory`"
        pay_query = "SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM `payment`"
        rent_query = "SELECT rental_id, rental_date, inventory_id, return_date FROM `rental`"

        table_to_csv(inv_query, inv_bronze)
        table_to_csv(rent_query, rent_bronze)
        table_to_csv(pay_query, paym_bronze)

    def transform(**kwargs):
        df_inv = csv_to_df(inv_bronze)
        df_rent = csv_to_df(rent_bronze)
        df_pay = csv_to_df(paym_bronze)

        df_ = pd.merge(
            pd.merge(
                df_pay,
                df_rent,
                on=['rental_id'],
                how="left"
            ),
            df_inv,
            on=["inventory_id"],
            how="left"
        )
        df_s = df_[["payment_id", "customer_id", "staff_id", "film_id", "store_id", "amount", "payment_date", "rental_date", "return_date"]]
        df_s1 = df_s.groupby(["customer_id", "staff_id", "film_id", "store_id", "payment_date", "rental_date", "return_date"]).aggregate({"amount": "sum", "payment_id": "count"})
        df_s2 = pd.concat([df_s.drop(['amount', 'payment_id'], axis = 1, inplace = True) , df_s1], axis=1)
        df_s2.to_csv(sales_date_silver, index=False)  

        df_ = df_[["payment_id", "customer_id", "staff_id", "film_id", "store_id", "amount"]]
        df_1 = df_.groupby(["customer_id", "staff_id", "film_id", "store_id"]).aggregate({ "amount": "sum", "payment_id": "count" })
        df_2 = pd.concat([df_ , df_1], axis=1)
        df_2.to_csv(sales_silver, index=False)  

    def load(**kwargs):
        csv_to_table(sales_fact, sales_silver)
        csv_to_table(sales_withdate_fact, sales_date_silver)


    extract_task = PythonOperator(task_id='extract',python_callable=extract,)
    transform_task = PythonOperator(task_id='transform', python_callable=transform,)
    load_task = PythonOperator(task_id='load', python_callable=load,)
    extract_task >> transform_task >> load_task

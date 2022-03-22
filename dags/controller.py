from airflow import DAG
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker
from airflow.operators.dummy_operator import DummyOperator

 

with DAG(dag_id="controller", schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Jakarta"), catchup=False, tags=["sehatq"]) as dag:
    
    address_task = TriggerDagRunOperator(
        task_id="address_task",
        trigger_dag_id="adress_silver",
        wait_for_completion=True
    )

    customer_task = TriggerDagRunOperator(
        task_id="customer_task",
        trigger_dag_id="customer_dim",
        wait_for_completion=True
    )
    
    staff_task = TriggerDagRunOperator(
        task_id="staff_task",
        trigger_dag_id="staff_dim",
        wait_for_completion=True
    )
    
    film_task = TriggerDagRunOperator(
        task_id="film_task",
        trigger_dag_id="film_dim",
        wait_for_completion=True
    )
    
    store_task = TriggerDagRunOperator(
        task_id="store_task",
        trigger_dag_id="store_dim",
        wait_for_completion=True
    )
    
    sales_fact_task = TriggerDagRunOperator(
        task_id="sales_fact_task",
        trigger_dag_id="sales_fact",
        wait_for_completion=True
    )
    
    ml_fact_task = TriggerDagRunOperator(
        task_id="ml_fact_task",
        trigger_dag_id="ml_fact",
        wait_for_completion=True
    )

    start_task = DummyOperator(task_id="start_task")
    end_task = DummyOperator(task_id="end_task")

    start_task >> address_task >> [customer_task, staff_task, store_task, film_task] >> sales_fact_task >> ml_fact_task >> end_task

    # address_task >> customer_task >> sales_fact_task >> end_task
    # address_task >> staff_task >> sales_fact_task >> end_task
    # address_task >> store_task >> sales_fact_task >> end_task
    # film_task >> sales_fact_task >> end_task
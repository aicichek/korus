import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    'DATAMART_create',
    default_args=default_args,
    description='Создание витрины',
    template_searchpath='/opt/airflow/sql/',
    schedule_interval=None,
    )

create_datamart_schema = PostgresOperator(
    task_id='datamart_schema_create',
    postgres_conn_id='conn_9_db',
    sql='datamart_schema_create.sql',
    dag=dag
    )

create_sales_mart = PostgresOperator(
    task_id='create_sales_mart',
    postgres_conn_id='conn_9_db',
    sql='datamart_sales.sql',
    dag=dag
    )

create_datamart_schema >> create_sales_mart
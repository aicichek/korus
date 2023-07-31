import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    'DDL_create',
    default_args=default_args,
    description='Создание схем и сущностей в БД',
    template_searchpath='/opt/airflow/sql/',
    schedule_interval=None,
    )

create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id='conn_9_db',
    sql='schemas_create.sql',
    dag=dag
    )

create_dds_tables = PostgresOperator(
    task_id='create_dds_tables',
    postgres_conn_id='conn_9_db',
    sql='dds_tables_create.sql',
    dag=dag
    )

create_damaged_data_tables = PostgresOperator(
    task_id='create_damaged_data_tables',
    postgres_conn_id='conn_9_db',
    sql='damaged_data_tables_create.sql',
    dag=dag
    )

trigger_DDS = TriggerDagRunOperator(
    task_id='trigger_DDS',
    trigger_dag_id='DDS_fill',
    dag=dag
)

create_schema >> create_dds_tables >> create_damaged_data_tables >> trigger_DDS
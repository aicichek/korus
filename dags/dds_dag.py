import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

conn_sources = PostgresHook(postgres_conn_id='conn_sources')
conn_9_db = PostgresHook(postgres_conn_id='conn_9_db')

os.environ['conn_sources'] = conn_sources.get_uri().rsplit('?')[0]
os.environ['conn_9_db'] = conn_9_db.get_uri().rsplit('?')[0]

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
    'DDS_fill',
    default_args=default_args,
    description='Обработка данных и занесение в таблицы',
    schedule_interval=None,
)

fill_brand = BashOperator(
    task_id='fill_brand',
    bash_command='python /opt/airflow/scripts/brand.py',
    dag=dag
)

fill_category = BashOperator(
    task_id='fill_category',
    bash_command='python /opt/airflow/scripts/category.py',
    dag=dag
)

fill_product = BashOperator(
    task_id='fill_product',
    bash_command='python /opt/airflow/scripts/product.py',
    dag=dag
)

fill_store = BashOperator(
    task_id='fill_store',
    bash_command='python /opt/airflow/scripts/store.py',
    dag=dag
)

fill_transaction = BashOperator(
    task_id='fill_transaction',
    bash_command='python /opt/airflow/scripts/transaction.py',
    dag=dag
)

fill_stock = BashOperator(
    task_id='fill_stock',
    bash_command='python /opt/airflow/scripts/stock.py',
    dag=dag
)

trigger_Datamart = TriggerDagRunOperator(
    task_id='trigger_Datamart',
    trigger_dag_id='DATAMART_create',
    dag=dag
)

fill_brand >> fill_category >> fill_product >> fill_store >> fill_transaction >> fill_stock >> trigger_Datamart

import os
import pandas as pd
from sqlalchemy import create_engine

# Получаем переменные окружения с данными о подключении к базе данных
conn_9_db_url = os.environ['conn_9_db']

# Создаем движок для подключения к базе данных
engine_9_db = create_engine(conn_9_db_url)

def load_stores_data():
    # Путь к файлу stores.csv
    csv_file_path = '/opt/airflow/scripts/stores.csv'

    # Загружаем данные из CSV файла в DataFrame
    df_stores = pd.read_csv(csv_file_path, sep=';', encoding='cp1251')

    # Записываем данные в таблицу store на схему dds
    df_stores.to_sql('store', engine_9_db, schema='dds', if_exists='append', index=False)

if __name__ == "__main__":
    load_stores_data()

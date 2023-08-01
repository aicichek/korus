import os
import pandas as pd
from sqlalchemy import create_engine

# Получаем переменные окружения с данными о подключении к базам данных
conn_sources_url = os.environ['conn_sources']
conn_9_db_url = os.environ['conn_9_db']

# Создаем движки для подключения к базам данных
engine_sources = create_engine(conn_sources_url)
engine_9_db = create_engine(conn_9_db_url)

def process_transaction_data():
    # Загружаем данные из CSV файла в DataFrame
    csv_file_path = "/opt/airflow/scripts/transaction.csv"  # Замените на путь к вашему CSV файлу
    df_transaction = pd.read_csv(csv_file_path)

    # Фильтруем строки с дубликатами transaction_id и записываем их в таблицу damaged_data
    df_damaged_data = df_transaction[df_transaction.duplicated(subset=['transaction_id'], keep=False)]

    # Удаляем дубликаты из основной таблицы
    df_transaction = df_transaction.drop_duplicates(subset=['transaction_id'], keep='first')

    # Фильтруем строки с некорректными данными и записываем их в таблицу damaged_data
    df_damaged_data = pd.concat([
        df_damaged_data,
        df_transaction[
            ~df_transaction['quantity'].astype(str).str.replace('.', '', 1).str.isdigit() |
            ~df_transaction['price'].astype(str).str.replace('.', '', 1).str.isdigit() |
            ~df_transaction['price_full'].astype(str).str.replace('.', '', 1).str.isdigit() |
            df_transaction['pos'].isna() |
            ~df_transaction['product_id'].astype(str).str.isdigit()
        ]
    ])

    # Получаем список существующих product_id из таблицы product
    existing_product_ids = get_existing_product_ids()

    # Фильтруем строки с product_id, которых нет в таблице product и записываем их в таблицу damaged_data
    df_damaged_data = pd.concat([
        df_damaged_data,
        df_transaction[~df_transaction['product_id'].isin(existing_product_ids)]
    ])

    # Удаляем строки с некорректными данными и product_id, которых нет в таблице product из основной таблицы
    df_transaction = df_transaction[
        df_transaction['quantity'].astype(str).str.replace('.', '', 1).str.isdigit() &
        df_transaction['price'].astype(str).str.replace('.', '', 1).str.isdigit() &
        df_transaction['price_full'].astype(str).str.replace('.', '', 1).str.isdigit() &
        df_transaction['product_id'].astype(str).str.isdigit() &
        df_transaction['pos'].notna() &
        df_transaction['product_id'].isin(existing_product_ids)
    ]
    # df_transaction = df_transaction[df_transaction['pos'].notna()]
    # Записываем данные в таблицу transaction на схему dds
    df_transaction.to_sql('transaction', engine_9_db, schema='dds', if_exists='append', index=False)

    # Записываем данные на схему damaged_data
    df_damaged_data.to_sql('transaction', engine_9_db, schema='damaged_data', if_exists='append', index=False)


def get_existing_product_ids():
    # Загружаем данные из таблицы product в DataFrame
    query = "SELECT product_id FROM dds.product"
    df_product = pd.read_sql_query(query, engine_9_db)
    return df_product['product_id'].tolist()


if __name__ == "__main__":
    process_transaction_data()

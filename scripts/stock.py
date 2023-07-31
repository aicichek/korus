import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Получаем переменные окружения с данными о подключении к базам данных
conn_sources_url = os.environ['conn_sources']
conn_9_db_url = os.environ['conn_9_db']

# Создаем движки для подключения к базам данных
engine_sources = create_engine(conn_sources_url)
engine_9_db = create_engine(conn_9_db_url)

def process_stock_data():
    # Загружаем данные из исходной таблицы stock в DataFrame
    query = "SELECT * FROM sources.stock"
    df_stock = pd.read_sql_query(query, engine_sources)

    # Получаем уникальные значения product_id из таблицы product
    valid_product_ids = get_product_ids()

    # Фильтруем и обрабатываем данные и записываем их в таблицу damaged_data
    df_damaged_data = df_stock[
        ~df_stock['product_id'].isin(valid_product_ids) |
        ~df_stock['cost_per_item'].astype(str).replace(',', '.', regex=True).str.replace('.', '', 1).str.isdigit() |
        ~df_stock['available_quantity'].astype(str).str.isdigit()
    ]

    # Удаляем строки с некорректными данными из основной таблицы
    df_stock = df_stock[
        df_stock['product_id'].isin(valid_product_ids) &
        df_stock['cost_per_item'].astype(str).replace(',', '.', regex=True).str.replace('.', '', 1).str.isdigit() &
        df_stock['available_quantity'].astype(str).str.isdigit()
    ]

    # Преобразуем столбец available_on из excel формата в формат гггг-мм-дд
    df_stock['available_on'] = df_stock['available_on'].apply(convert_to_date)

    # Записываем корректные данные в таблицу stock на схему dds
    df_stock.to_sql('stock', engine_9_db, schema='dds', if_exists='append', index=False)

    # Записываем некорректные данные в таблицу stock на схему damaged_data
    df_damaged_data.to_sql('stock', engine_9_db, schema='damaged_data', if_exists='append', index=False)


def get_product_ids():
    # Загружаем данные из таблицы product в DataFrame
    query = "SELECT product_id FROM dds.product"
    df_product = pd.read_sql_query(query, engine_9_db)
    return df_product['product_id'].tolist()


def convert_to_date(serial_date):
    origin_date = datetime(1899, 12, 30)
    return origin_date + timedelta(days=int(serial_date))


if __name__ == "__main__":
    process_stock_data()

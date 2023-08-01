import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from xlrd import xldate_as_datetime

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
    df_stock.drop_duplicates(ignore_index=False, keep='first', inplace=True)
    df_stock = df_stock.drop_duplicates(subset=['product_id', 'pos', 'available_on'], keep='first')
    # Получаем уникальные значения product_id из таблицы product
    valid_product_ids = get_product_ids()

    # Фильтруем и обрабатываем данные и записываем их в таблицу damaged_data
    df_damaged_data = df_stock[
        (df_stock['product_id'] == "") |
        (df_stock['cost_per_item'] == "") |
        (df_stock['available_quantity'] < "0") |
        (df_stock['pos'] == 'Магазин 999')
    ]

    # Удаляем строки с некорректными данными из основной таблицы
    df_stock = df_stock[
        (df_stock['product_id'] != "") &
        (df_stock['cost_per_item'] != "") &
        (df_stock['available_quantity'] >= "0") &
        (df_stock['pos'] != 'Магазин 999')
    ]
    df_stock['product_id'] = df_stock['product_id'].astype(float).astype(int)
    df_stock=df_stock[(df_stock['product_id'].astype(int).isin(valid_product_ids))]
    df_stock['available_quantity'] = df_stock['available_quantity'].astype(float).astype(int)
    df_stock['available_on'] = df_stock['available_on'].astype(int).apply(convert_to_date)
    df_stock['available_on'] = pd.to_datetime(df_stock['available_on'], format='%Y-%m-%d').astype(str)
   

    # Записываем корректные данные в таблицу stock на схему dds
    df_stock.to_sql('stock', engine_9_db, schema='dds', if_exists='append', index=False)

    # Записываем некорректные данные в таблицу stock на схему damaged_data
    df_damaged_data.to_sql('stock', engine_9_db, schema='damaged_data', if_exists='append', index=False)
    # return df_stock, df_damaged_data

def get_product_ids():
    # Загружаем данные из таблицы product в DataFrame
    query = "SELECT * FROM dds.product"
    df_product = pd.read_sql_query(query, engine_9_db, index_col="product_id")
    return df_product.index.values


def convert_to_date(num: int):
    return xldate_as_datetime(num, 0).date().isoformat()


if __name__ == "__main__":
    process_stock_data()

import os
import pandas as pd
from sqlalchemy import create_engine

# Получаем переменные окружения с данными о подключении к базам данных
conn_sources_url = os.environ['conn_sources']
conn_9_db_url = os.environ['conn_9_db']

# Создаем движки для подключения к базам данных
engine_sources = create_engine(conn_sources_url)
engine_9_db = create_engine(conn_9_db_url)

def process_product_data():
    # Загружаем данные из исходной таблицы product в DataFrame
    query = "SELECT * FROM sources.product"
    df_product = pd.read_sql_query(query, engine_sources)

    # Получаем уникальные значения 
    valid_category_ids = get_category_ids()
    valid_brand_ids = get_brand_ids()

    # Фильтруем строки с некорректными данными и записываем их в таблицу damaged_data
    df_damaged_data = df_product[
        ~df_product['product_id'].astype(str).str.isdigit() |
        ~df_product['pricing_line_id'].astype(str).str.isdigit() |
        ~df_product['brand_id'].isin(valid_brand_ids) |
        ~df_product['category_id'].isin(valid_category_ids) |
        df_product.duplicated(subset=['name_short', 'brand_id'], keep=False)
    ]

    # Удаляем строки с некорректными данными из основной таблицы
    df_product = df_product[
        df_product['product_id'].astype(str).str.isdigit() &
        df_product['pricing_line_id'].astype(str).str.isdigit() &
        df_product['brand_id'].isin(valid_brand_ids) &
        df_product['category_id'].isin(valid_category_ids) &
        ~df_product.duplicated(subset=['name_short', 'brand_id'], keep=False)
    ]

    # Записываем корректные данные в таблицу product на схему dds
    df_product.to_sql('product', engine_9_db, schema='dds', if_exists='append', index=False)

    # Записываем некорректные данные в таблицу product на схему damaged_data
    df_damaged_data.to_sql('product', engine_9_db, schema='damaged_data', if_exists='append', index=False)


def get_brand_ids():
    # Загружаем данные из таблицы brand в DataFrame
    query = "SELECT brand_id FROM sources.brand"
    df_brand = pd.read_sql_query(query, engine_sources)
    return df_brand['brand_id'].tolist()


def get_category_ids():
    # Загружаем данные из таблицы category в DataFrame
    query = "SELECT category_id FROM sources.category"
    df_category = pd.read_sql_query(query, engine_sources)
    return df_category['category_id'].tolist()


if __name__ == "__main__":
    process_product_data()

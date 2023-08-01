import os
import pandas as pd
from sqlalchemy import create_engine

# Получаем переменные окружения с данными о подключении к базам данных
conn_sources_url = os.environ['conn_sources']
conn_9_db_url = os.environ['conn_9_db']

# Создаем движки для подключения к базам данных
engine_sources = create_engine(conn_sources_url)
engine_9_db = create_engine(conn_9_db_url)

def process_brand_data():
    print(os.environ['conn_sources'],os.environ['conn_9_db'])
    # Загружаем данные из исходной таблицы brand в DataFrame
    query = "SELECT * FROM sources.brand"
    df_brand = pd.read_sql_query(query, engine_sources)

    # Удаляем дубликаты из основной таблицы
    df_brand = df_brand.drop_duplicates(subset=['brand_id'], keep='first')

    # Фильтруем строки с некорректными данными и записываем их в таблицу damaged_data
    df_damaged_data = df_brand[~df_brand['brand_id'].astype(str).str.isdigit()]
    df_damaged_data = df_brand[df_brand['brand'] == "NO BRAND"]

    # Удаляем строки с некорректными данными из основной таблицы
    df_brand = df_brand[df_brand['brand_id'].astype(str).str.isdigit()]
    df_brand = df_brand[df_brand['brand'] != "NO BRAND"]


    # Записываем корректные данные в таблицу brand на схему dds
    df_brand.to_sql('brand', engine_9_db, schema='dds', if_exists='append', index=False)

    # Записываем некорректные данные в таблицу brand на схему damaged_data
    df_damaged_data.to_sql('brand', engine_9_db, schema='damaged_data', if_exists='append', index=False)


if __name__ == "__main__":
    process_brand_data()

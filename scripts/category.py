import os
import pandas as pd
from sqlalchemy import create_engine

# Получаем переменные окружения с данными о подключении к базам данных
conn_sources_url = os.environ['conn_sources']
conn_9_db_url = os.environ['conn_9_db']

# Создаем движки для подключения к базам данных
engine_sources = create_engine(conn_sources_url)
engine_9_db = create_engine(conn_9_db_url)

def process_category_data():
    # Загружаем данные из исходной таблицы category в DataFrame
    query = "SELECT * FROM sources.category"
    df_category = pd.read_sql_query(query, engine_sources)

    # Удаляем дубликаты из основной таблицы
    df_category = df_category.drop_duplicates(subset=['category_id'], keep='first')

    # Записываем данные в таблицу category на схему dds
    df_category.to_sql('category', engine_9_db, schema='dds', if_exists='append', index=False)


if __name__ == "__main__":
    process_category_data()

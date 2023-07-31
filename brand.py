import psycopg2
import os

# Подключение к conn_sources
conn_sources = psycopg2.connect(os.environ['conn_sources'])
cursor_sources = conn_sources.cursor()

# Подключение к conn_9_db
conn_9_db = psycopg2.connect(os.environ['conn_9_db'])
cursor_9_db = conn_9_db.cursor()

try:
    # Получаем все данные из таблицы brand в схеме sources
    cursor_sources.execute("SELECT * FROM sources.brand")
    rows = cursor_sources.fetchall()

    # Удаляем дубликаты
    unique_rows = []
    seen_ids = set()
    for row in rows:
        brand_id, brand = row
        if brand_id not in seen_ids:
            unique_rows.append((brand_id, brand))
            seen_ids.add(brand_id)

    # Разделяем данные по заданной логике
    damaged_data_rows = [(brand_id, brand) for brand_id, brand in unique_rows if not brand_id.isdigit()]
    dds_rows = [(brand_id, brand) for brand_id, brand in unique_rows if brand_id.isdigit()]

    # Заносим данные на схему damaged_data в таблицу brand
    cursor_9_db.executemany("INSERT INTO damaged_data.brand (brand_id, brand) VALUES (%s, %s)", damaged_data_rows)

    # Заносим данные на схему dds в таблицу brand
    cursor_9_db.executemany("INSERT INTO dds.brand (brand_id, brand) VALUES (%s, %s)", dds_rows)

    # Фиксируем изменения и закрываем соединения
    conn_9_db.commit()

except psycopg2.Error as e:
    print("Error:", e)

finally:
    cursor_sources.close()
    cursor_9_db.close()
    conn_sources.close()
    conn_9_db.close()

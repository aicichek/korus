import psycopg2
import os

# Подключение к conn_sources
conn_sources = psycopg2.connect(os.environ['conn_sources'])
cursor_sources = conn_sources.cursor()

# Подключение к conn_9_db
conn_9_db = psycopg2.connect(os.environ['conn_9_db'])
cursor_9_db = conn_9_db.cursor()

try:
    # Получаем все данные из таблицы category в схеме sources
    cursor_sources.execute("SELECT * FROM sources.category")
    rows = cursor_sources.fetchall()

    # Удаляем дубликаты
    unique_rows = []
    seen_ids = set()
    for row in rows:
        category_id, category = row
        if category_id not in seen_ids:
            unique_rows.append((category_id, category))
            seen_ids.add(category_id)

    # Заносим данные на схему dds в таблицу category
    cursor_9_db.executemany("INSERT INTO dds.category (category_id, category) VALUES (%s, %s)", unique_rows)

    # Фиксируем изменения и закрываем соединения
    conn_9_db.commit()

except psycopg2.Error as e:
    print("Error:", e)

finally:
    cursor_sources.close()
    cursor_9_db.close()
    conn_sources.close()
    conn_9_db.close()

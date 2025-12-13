import pandas as pd
from sqlalchemy import create_engine
from clickhouse_driver import Client
import os

def main():
    pg_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/declaration_db') # URL PostgreSQL

    pg = create_engine('pg_url') # подключение к PostgreSQL

    ch = Client(
        host='rc1b-dghi9mds291db82e.mdb.yandexcloud.net',
        port=8443,
        user='admin',
        password='21041984',
        database='declaration_db',
        secure=False
    )

    print("Создание таблицы...")
    with open('clickhouse_schema.sql', 'r') as f:
        schema_sql = f.read()
        ch.execute(schema_sql)

    print("Загрузка данных из PostrgreSQL...")
    df = pd.read_sql("SELECT * FROM dm_dashboard_main", pg)

    if len(df) > 0:
        ch.execute("TRUNCATE TABLE dm_dashboard_main") # очистка таблицы
        ch.execute("INSERT INTO dm_dashboard_main VALUES", df.to_dict('records')) # вставка данных
        print(f"Данные загружены в Clickhouse")

    print("Добавление вычисляемых полей...")
    ch.execute("""
    ALTER TABLE dm_dashboard_main 
    ADD COLUMN IF NOT EXISTS avg_property_size Float64 MATERIALIZED
        IF(own_properties_count > 0, total_own_m2 / own_properties_count, 0)
    """)

    ch.execute("""
    ALTER TABLE dm_dashboard_main
    ADD COLUMN IF NOT EXISTS has_foreign_property UInt8 MATERIALIZED
        IF(foreign_properties > 0, 1, 0)
    """)

    pg.dispose()
    ch.disconnect()

    print("Готово для Datalens!")

if __name__ == "__main__":
    main()
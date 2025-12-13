import pandas as pd
import numpy as np
from pathlib import Path
import json
import os

from sqlalchemy.inspection import inspect
from models.data_vault import split_and_hash, prepare_person_hub, prepare_declaration_hub, prepare_asset_hub
from models.data_vault import Hub, Sat, Link

from models import person
from models import declaration
from models import asset

from sqlalchemy import create_engine

def get_object_fields(object, keep_hash):
    if keep_hash:
        result = []
    else:
        result = []

    for column in inspect(object).c:
        if column.name == 'load_date' or column.name == 'source':
            continue

        if column.name == 'valid_from' or column.name == 'valid_to':
            continue

        if '_hash' in column.name and keep_hash == False:
            continue

        result.append(column.name)
    return result

#def split_data(data): # NaN и убираем разделитель '|'
    #if data is np.nan:
        #return [np.nan, np.nan]

    #result = data.split('|')

    #return result

def transform():
    print("Создание папок...")
    Path("stage").mkdir(parents=True, exist_ok=True) 
    Path("stage/data").mkdir(parents=True, exist_ok=True)
    Path("stage/hubs").mkdir(parents=True, exist_ok=True)
    Path("stage/links").mkdir(parents=True, exist_ok=True)
    Path("stage/sats").mkdir(parents=True, exist_ok=True)
    Path("stage/dict").mkdir(parents=True, exist_ok=True)
    Path("sql").mkdir(parents=True, exist_ok=True)
    Path("scripts").mkdir(parents=True, exist_ok=True)
    print("Структура папок создана!")

    df = pd.read_csv('df_all.csv')

    #with open('field_mapping.jsonc', 'r', encoding='utf-8') as f:  # переименование полей согласно field_mapping.jsonc
        #field_map = json.load(f)

    #display_df = df.rename(columns=field_map)

    prepare_asset_hub(df, source='DECLARATIONS') # asset хаб со всеми данными

    hash_tasks = [

        (df, 'person',
            get_object_fields(person.SatPerson, False), 'DECLARATIONS'
        ),
        (df, 'declaration',
            get_object_fields(declaration.SatDeclaration, False), 'DECLARATIONS'
        ),
    ]

    for hash_task in hash_tasks:
        split_and_hash(*hash_task)

    hub_tasks = [
        ('person',),
        ('declaration',),
        ('asset',)
    ]

    for hub_task in hub_tasks:
        Hub.prepare_hub(*hub_task)

    sat_tasks = [
        ('person', get_object_fields(person.SatPerson, True), None),
        ('declaration', get_object_fields(declaration.SatDeclaration, True), None),
        ('asset', get_object_fields(asset.SatAssetOwnRealty, True), 'OWN_REALTY'),
        ('asset', get_object_fields(asset.SatAssetUseRealty, True), 'USE_REALTY'),
        ('asset', get_object_fields(asset.SatAssetCar, True), 'CAR')
    ]

    for sat_task in sat_tasks:
        Sat.prepare_sat(*sat_task)

    link_tasks = [
        ('person', 'declaration', 'l_person_declaration', 'DECLARATIONS'),
        ('declaration', 'asset', 'l_declaration_asset', 'DECLARATIONS')
    ]

    for link_task in link_tasks:
        Link.prepare_links(*link_task)

def check_csv_files():
    print("="*50)
    print("СОЗДАННЫЕ ФАЙЛЫ:")

    for folder in ["hubs", "sats", "links", "data"]:
        path = f"stage/{folder}/"
        if os.path.exists(path):
            files = os.listdir(path)
            print(f"\n{folder.upper()} ({len(files)} файлов):")
            for f in files:
                print(f"  - {f}")
        else:
            print(f"\n{folder.upper()}: папка не существует")
    print("="*50)

def load():
    engine = create_engine("postgresql://postgres:postgres@postgres:5432/declaration_db")

    print("Загрузка в PostgreSQL")
    print("\n" + "="*50)

    hub_path = "stage/hubs/"
    for filename in os.listdir(hub_path):
        df = pd.read_csv(hub_path + filename)
        table = filename.replace('.csv', '')
        df.to_sql(table, con=engine, if_exists='replace', index=False)
        print(f" Загружен хаб: {table} ({len(df)} записей)")

    link_path = "stage/links/"
    for filename in os.listdir(link_path):
        df = pd.read_csv(link_path + filename)
        table = filename.replace('.csv', '')
        df.to_sql(table, con=engine, if_exists='replace', index=False)
        print(f" Загружен линк: {table} ({len(df)} записей)")

    sat_path = "stage/sats/"
    for filename in os.listdir(sat_path):
        df = pd.read_csv(sat_path + filename)
        table = filename.replace('.csv', '')
        df.to_sql(table, con=engine, if_exists='replace', index=False)
        print(f" Загружен сателлит: {table} ({len(df)} записей)")

    print("\n Все данные загружены в PostgreSQL")
    print("="*50)
    return engine

def create_dashboard_view(engine):
    try:
        with open('sql/dashboard_view.sql', 'r', encoding='utf-8') as f:  # чтение SQL-скрипта из файла
            sql_script = f.read()

        with open('sql/indexes.sql', 'r', encoding='utf-8') as f:
            indexes_sql = f.read()

        with engine.connect() as conn: # создание соединения через sqlalchemy
            from sqlalchemy import text

            conn.execute(text("DROP VIEW IF EXISTS dm_dashboard_main CASCADE"))
            conn.execute(text(sql_script))
            conn.execute(text(indexes_sql))
            conn.commit()

            print(f"VIEW dm_dashboard_main успешно создан")

            #result = conn.execute(text("SELECT COUNT(*) FROM dm_dashboard_main"))
            #count = result.fetchone()[0]
            #print(f"В VIEW содержится {count} записей")

    except Exception as e:
        print(f"Ошибка при создании VIEW: {e}")
        raise

if __name__ == "__main__":
    transform()
    check_csv_files()
    engine = load()
    create_dashboard_view(engine)

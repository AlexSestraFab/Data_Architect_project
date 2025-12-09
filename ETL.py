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

def show_created_files():
    print("\n" + "="*50)
    print("СОЗДАННЫЕ ФАЙЛЫ:")
    print("="*50)

    for folder in ["hubs", "sats", "links", "data"]:
        path = f"stage/{folder}/"
        if os.path.exists(path):
            files = os.listdir(path)
            print(f"\n{folder.upper()} ({len(files)} файлов):")
            for f in files:
                print(f"  - {f}")
        else:
            print(f"\n{folder.upper()}: папка не существует")

    print("\n" + "="*50)

def load():
    engine = create_engine("sqlite:///test_declarations.db")

    hub_path = "stage/hubs/"
    for filename in os.listdir(hub_path):
        df = pd.read_csv(hub_path + filename)
        table = filename.replace('.csv', '')
        df.to_sql(table, con=engine, if_exists='append', index=False)

    link_path = "stage/links/"
    for filename in os.listdir(link_path):
        df = pd.read_csv(link_path + filename)
        table = filename.replace('.csv', '')
        df.to_sql(table, con=engine, if_exists='append', index=False)

    sat_path = "stage/sats/"
    for filename in os.listdir(sat_path):
        df = pd.read_csv(sat_path + filename)
        table = filename.replace('.csv', '')
        df.to_sql(table, con=engine, if_exists='append', index=False)

        with engine.connect() as conn:
            result = conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = [row[0] for row in result]
        
            print(f"Создано таблиц: {len(tables)}")
        print("Таблицы:", ", ".join(sorted(tables)))

        print("Данные успешно загружены в SQLite базу test_declarations.db")

transform()
load()
show_created_files()
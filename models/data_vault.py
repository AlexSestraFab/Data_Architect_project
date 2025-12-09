import pandas as pd
from models.utils import generate_hash_key
from datetime import datetime
import os

def split_and_hash(data, object_name, business_key_columns, source, separator='_'):
    object_data = data[business_key_columns].copy()

    object_data[object_name + '_business_key'] = object_data[business_key_columns].astype(str).agg(separator.join, axis=1)
    object_data[object_name + '_hash'] = object_data[object_name + '_business_key'].apply(lambda x: generate_hash_key(x) if pd.notna(x) else None)
    object_data['load_date'] = datetime.now()
    object_data['source'] = source

    object_data.to_csv(f'stage/data/{object_name}.csv', index=False)
    return object_data

def prepare_person_hub(data, source='declarations'):
    result = split_and_hash(data=data, object_name='person', business_key_columns=['name', 'state_agency', 'gender'], source=source, separator='_')
    return result # изменения 3 (только строка 'result =' и эта строка)

def prepare_declaration_hub(data, source='declarations'):
    result = split_and_hash(data=data, object_name='declaration', business_key_columns=['name', 'state_agency', 'year', 'family'], source=source, separator='_')
    return result # изменения 3 (только строка 'result =' и эта строка)

def prepare_asset_hub(data, source='declarations'): # изменения 4 (далее вниз до конца блока - разделение на OWN_R/USE_R/CAR)
    data_copy = data.copy()

    data_copy['asset_type'] = None # изменения 6 (переделано разделение на OWN_R/USE_R/CAR)
    data_copy['asset_business_key'] = None # создание столбцов 

    own_mask = data_copy['type'].notna() & data_copy['own_type'].notna() # маска для own_realty
    data_copy.loc[own_mask, 'asset_type'] = 'OWN_REALTY'
    data_copy.loc[own_mask, 'asset_business_key'] = 'OWN_REALTY|' + data_copy['type'] + '|' + data_copy['year'].astype(str)    

    use_mask = data_copy['use_type'].notna() # маска для use_realty
    data_copy.loc[use_mask, 'asset_type'] = 'USE_REALTY'
    data_copy.loc[use_mask, 'asset_business_key'] = 'USE_REALTY|' + data_copy['use_type'] + '|' + data_copy['year'].astype(str)

    car_mask = data_copy['car'].notna() # маска для car
    data_copy.loc[car_mask, 'asset_type'] = 'CAR'
    data_copy.loc[car_mask, 'asset_business_key'] = 'CAR|' + data_copy['car'] + '|' + data_copy['year'].astype(str)

    all_assets = data_copy[data_copy['asset_type'].notna()].copy() # фильтрация строк с активами

    all_assets['type'] = data_copy['type'] # сохранение значений в отфильтр. data_copy
    all_assets['own_type'] = data_copy['own_type']
    all_assets['use_type'] = data_copy['use_type']
    all_assets['car'] = data_copy['car']
    all_assets['year'] = data_copy['year']

    for col in ['meters', 'country', 'use_meters', 'use_country', 'car_brands']: # копирование доп. полей
        if col in data_copy.columns:
            all_assets[col] = data_copy[col]

    all_assets['asset_hash'] = all_assets['asset_business_key'].apply(lambda x: generate_hash_key(x) if pd.notna(x) else None) 
    all_assets['load_date'] = datetime.now() # добавление метаданных (и выше хэш-ключа на основе бизнес-ключа)
    all_assets['source'] = source

    all_assets.to_csv(f'stage/data/asset.csv', index=False)
    return all_assets

def prepare_position_dictionary(data, source='declarations'): # изменения 1 (весь блок define position_id)
    position_data = data[['position', 'position_standard', 'position_category', 'position_group']].copy()
    position_data = position_data.drop_duplicates()
    position_data['position_id'] = position_data[['position', 'position_standard', 'position_category', 'position_group']].astype(str).agg('|'.join, axis=1)
    position_data.to_csv('stage/dictionaries/d_position.csv', index=False)
    return position_data

def prepare_agency_dictionary(data, source='declarations'): # изменения 2 (весь блок define position_id)
    agency_data = data[['state_agency', 'state_agency_short', 'state_agency_full']].copy()
    agency_data = agency_data.drop_duplicates()
    agency_data['agency_id'] = agency_data[['state_agency', 'state_agency_short', 'state_agency_full']].astype(str).agg('|'.join, axis=1)
    agency_data.to_csv('stage/dictionaries/d_agency.csv', index=False)
    return agency_data

class Hub():
    @staticmethod
    def prepare_hub(object_name):
        object_df = pd.read_csv(f'stage/data/{object_name}.csv')
        hub_df = object_df[[object_name + '_hash', object_name + '_business_key', 'load_date', 'source']]
        hub_df = hub_df.drop_duplicates(subset=[object_name + '_hash'])
        hub_df.to_csv(f'stage/hubs/h_{object_name}.csv', index=False)

class Sat():
    @staticmethod
    def prepare_sat(object_name, column_list, asset_type=None):
        object_df = pd.read_csv(f'stage/data/{object_name}.csv')

        if asset_type and 'asset_type' in object_df.columns: 
            object_df = object_df[object_df['asset_type'] == asset_type] # фильтрация по типу актива

        column_list += [object_name + '_hash', 'load_date', 'source']
        sat_df = object_df[column_list]
        sat_df = sat_df.drop_duplicates(subset=[object_name + '_hash'])

        filename = f'stage/sats/s_{object_name}'
        if asset_type:
            filename += f'_{asset_type.lower()}' # добавляет тип актива в имя файла (формирует путь)
        filename += '.csv'

        sat_df.to_csv(filename, index=False)

class Link():
    @staticmethod
    def prepare_links(object1, object2, link_tbname, source):

        one_df = pd.read_csv(f'stage/data/{object1}.csv')
        other_df = pd.read_csv(f'stage/data/{object2}.csv')

        link_hash = one_df[object1 + '_hash'] + other_df[object2 + '_hash']

        link_df = pd.DataFrame({'link_hash': link_hash.apply(lambda x: generate_hash_key(x) if pd.notna(x) else None),
                                object1 + '_hash': one_df[object1 + '_hash'], object2 + '_hash': other_df[object2 + '_hash']
        })

        link_df['load_date'] = datetime.now()
        link_df['source'] = source

        link_df.to_csv(f'stage/links/{link_tbname}.csv', index=False)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, Integer, JSON, DateTime, ForeignKey

from models.data_vault import Hub, Sat

class Base(DeclarativeBase): pass

class HubAsset(Base, Hub):
    __tablename__ = "h_asset"
    __obj__ = 'asset'

    asset_hash = Column(String, primary_key=True)
    asset_business_key = Column(String)
    load_date = Column(DateTime)
    source = Column(String)

class SatAssetOwnRealty(Base, Sat):
    __tablename__ = "s_asset_own_realty"
    asset_hash = Column(String, ForeignKey(HubAsset.asset_hash), primary_key=True, nullable=False)

    meters = Column(String)
    country = Column(String)
    own_type = Column(String)
    year = Column(Integer)

    valid_from = Column(DateTime)
    valid_to = Column(DateTime)
    load_date = Column(DateTime)
    source = Column(String)

class SatAssetUseRealty(Base, Sat):
    __tablename__ = "s_asset_use_realty"
    asset_hash = Column(String, ForeignKey(HubAsset.asset_hash), primary_key=True, nullable=False)

    use_meters = Column(String)
    use_country = Column(String)
    use_type = Column(String)
    year = Column(Integer)

    valid_from = Column(DateTime)
    valid_to = Column(DateTime)
    load_date = Column(DateTime)
    source = Column(String)

class SatAssetCar(Base, Sat):
    __tablename__ = "s_asset_car"
    asset_hash = Column(String, ForeignKey(HubAsset.asset_hash), primary_key=True, nullable=False)

    car = Column(String)
    car_brands = Column(JSON)
    year = Column(Integer)

    valid_from = Column(DateTime)
    valid_to = Column(DateTime)
    load_date = Column(DateTime)
    source = Column(String)
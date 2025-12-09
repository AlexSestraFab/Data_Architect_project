from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, DateTime, ForeignKey

from models.declaration import HubDeclaration
from models.asset import HubAsset

from models.data_vault import Link

class Base(DeclarativeBase): pass

class LinkDeclarationAsset(Base, Link):
    __tablename__ = "l_declaration_asset"

    l_declaration_asset_hash = Column(String, primary_key=True)
    declaration_hash = Column(String, ForeignKey(HubDeclaration.declaration_hash), nullable=False)
    asset_hash = Column(String, ForeignKey(HubAsset.asset_hash), nullable=False)
    load_date = Column(DateTime)
    source = Column(String)
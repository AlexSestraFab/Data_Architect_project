from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, DateTime, Numeric, ForeignKey

from models.data_vault import Hub, Sat
from models.dict import DictAgency

class Base(DeclarativeBase): pass

class HubDeclaration(Base, Hub):
    __tablename__ = "h_declaration"
    __obj__ = 'declaration'

    declaration_hash = Column(String, primary_key=True)
    declaration_business_key = Column(String)
    load_date = Column(DateTime)
    source = Column(String)

class SatDeclaration(Base, Sat):
    __tablename__ = "s_declaration"
    declaration_hash = Column(String, ForeignKey(HubDeclaration.declaration_hash), primary_key=True, nullable=False)

    income_чиновник = Column(Numeric)
    income_супруга = Column(Numeric)
    income_ребенок = Column(Numeric)
    source_sum_чиновник = Column(Numeric)
    source_sum_супруга = Column(Numeric)
    source_sum_ребенок = Column(Numeric)
    income_diff_чиновник = Column(Numeric)
    income_diff_супруга = Column(Numeric)
    income_diff_ребенок = Column(Numeric)
    income_month_const_чиновник = Column(Numeric)
    income_month_const_супруга = Column(Numeric)
    income_month_const_ребенок = Column(Numeric)
    extra = Column(String)
    coef = Column(Numeric)

    valid_from = Column(DateTime)
    valid_to = Column(DateTime)
    load_date = Column(DateTime)
    source = (String)
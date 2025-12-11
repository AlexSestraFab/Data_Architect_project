from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, DateTime, String, ForeignKey

from models.data_vault import Hub, Sat
from models.dict import DictPosition

class Base(DeclarativeBase): pass

class HubPerson(Base, Hub):
    __tablename__ = "h_person"
    __obj__ = 'person'

    person_hash = Column(String, primary_key=True)
    person_business_key = Column(String)
    load_date = Column(DateTime)
    source = Column(String)

class SatPerson(Base, Sat):

    __tablename__ = "s_person"
    person_hash = Column(String, ForeignKey(HubPerson.person_hash), primary_key=True, nullable=False)

    name = Column(String)
    gender = Column(String)
    married = Column(String)
    children = Column(Integer)
    position = Column(String) # это словарь НАДО ПРОВЕРИТЬ!!
    state_agency_short = Column(String)

    valid_from = Column(DateTime)
    valid_to = Column(DateTime)
    load_date = Column(DateTime)
    source = Column(String)
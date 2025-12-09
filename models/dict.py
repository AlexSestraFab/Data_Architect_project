from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String

class Base(DeclarativeBase): pass

class DictPosition(Base):
    __tablename__ = "d_position"
    position_id = Column(String, primary_key=True, nullable=False)
    position = Column(String)
    position_standard = Column(String)
    position_category = Column(String)
    position_group = Column(String)

class DictAgency(Base):
    __tablename__ = "d_agency"
    agency_id = Column(String, primary_key=True, nullable=False)
    state_agency = Column(String)
    state_agency_short = Column(String)
    state_agency_full = Column(String)
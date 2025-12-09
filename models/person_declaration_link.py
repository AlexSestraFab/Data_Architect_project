from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, DateTime, ForeignKey

from models.person import HubPerson
from models.declaration import HubDeclaration

from models.data_vault import Link

class Base(DeclarativeBase): pass

class LinkPersonDeclaration(Base, Link):
    __tablename__ = "l_person_declaration"

    l_person_declaration_hash = Column(String, primary_key=True)
    person_hash = Column(String, ForeignKey(HubPerson.person_hash), nullable=False)
    declaration_hash = Column(String, ForeignKey(HubDeclaration.declaration_hash), nullable=False)
    load_date = Column(DateTime)
    source = Column(String)
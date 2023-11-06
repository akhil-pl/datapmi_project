from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Connections(Base):
    __tablename__ = 'connections'
    id = Column(Integer, autoincrement=True, primary_key=True)
    source = Column(String(255), nullable=False)
    host = Column(String(255), nullable=False)
    user = Column(String(255), nullable=False)
    password = Column(String(255))
    port = Column(String(255), nullable=False)
    database = Column(String(255), nullable=False)
    
    
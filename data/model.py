from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Connections(Base):
    __tablename__ = 'connections'
    id = Column(Integer, autoincrement=True, primary_key=True)
    source = Column(String(255))
    credentials = Column(String(255), nullable=False)
    created_on = Column(DateTime)
    status = Column(Boolean)
    last_connected = Column(DateTime)
    last_modified = Column(DateTime)
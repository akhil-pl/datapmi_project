from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

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
    


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    description = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())


class PipelineStatus(Base):
    __tablename__ = "pipeline_status"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id"))
    status = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())

    pipeline = relationship("Pipeline", back_populates="statuses")

Pipeline.statuses = relationship("PipelineStatus", order_by=PipelineStatus.created_at, back_populates="pipeline")

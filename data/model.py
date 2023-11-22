from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Text, DECIMAL, Enum, func
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Connections(Base):
    __tablename__ = 'connections'

    id = Column(Integer, autoincrement=True, primary_key=True)
    # connection_id = Column(Integer, autoincrement=True, primary_key=True)
    # connection_name = Column(String(255), nullable=False)
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





# class IngestionMetadata(Base):
#     __tablename__ = 'ingestionMetadata'

#     ingestion_id = Column(Integer, primary_key=True, autoincrement=True)
#     job_id = Column(Integer, ForeignKey('jobMetadata.job_id'), nullable=False)
#     source_name = Column(String(255), nullable=False)
#     ingestion_datetime = Column(DateTime, nullable=False)
#     executed_at = Column(DateTime, nullable=False)
#     updated_by = Column(String(255), nullable=False)
    
#     job = relationship('JobMetadata', back_populates='ingestions')


# class TransformationMetadata(Base):
#     __tablename__ = 'transformationMetadata'

#     transformation_id = Column(Integer, primary_key=True, autoincrement=True)
#     ingestion_id = Column(Integer, ForeignKey('ingestionMetadata.ingestion_id'), nullable=False)
#     job_id = Column(Integer, ForeignKey('jobMetadata.job_id'), nullable=False)
#     transformation_start_datetime = Column(DateTime, nullable=False)
#     transformation_end_datetime = Column(DateTime)
#     status = Column(Enum('In Progress', 'Completed', 'Failed'), nullable=False)
#     error_message = Column(Text)

#     job = relationship('JobMetadata', back_populates='transformations')
#     ingestion = relationship('IngestionMetadata')




# class MetricsMetadata(Base):
#     __tablename__ = 'metricsMetadata'

#     metric_id = Column(Integer, primary_key=True, autoincrement=True)
#     transformation_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), nullable=False)
#     metric_name = Column(String(255), nullable=False)
#     metric_value = Column(DECIMAL(10, 2), nullable=False)
#     transformation = relationship('TransformationMetadata')




# class PipelineMetadata(Base):
#     __tablename__ = 'pipelineMetadata'

#     pipeline_id = Column(Integer, primary_key=True, autoincrement=True)
#     pipeline_name = Column(String(255), nullable=False)
#     job_id = Column(Integer, ForeignKey('jobMetadata.job_id'), nullable=False)
#     pipeline_start_datetime = Column(DateTime, nullable=False)
#     pipeline_end_datetime = Column(DateTime)
#     status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
#     error_message = Column(Text)

#     job = relationship('JobMetadata', back_populates='pipelines')



# class PipelineExecution(Base):
#     __tablename__ = 'pipelineExecution'

#     execution_id = Column(Integer, primary_key=True, autoincrement=True)
#     pipeline_id = Column(Integer, ForeignKey('pipelineMetadata.pipeline_id'), nullable=False)
#     transformation_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), nullable=False)
#     execution_start_datetime = Column(DateTime, nullable=False)
#     execution_end_datetime = Column(DateTime)
#     status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
#     error_message = Column(Text)
#     pipeline = relationship('PipelineMetadata')
#     transformation = relationship('TransformationMetadata')



class JobMetadata(Base):
    __tablename__ = 'jobMetadata'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(255), nullable=False)
    job_type = Column(Enum('Ingestion', 'Transformation', 'Pipeline'), nullable=False)
    creation_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(String(255), nullable=False)

    # Establishing one-to-many relationship with child tables
    ingestions = relationship('IngestionMetadata', back_populates='job')
    transformations = relationship('TransformationMetadata', back_populates='job')
    pipelines = relationship('PipelineMetadata', back_populates='job')



class JobExecutionStatus(Base):
    __tablename__ = 'jobExecutionStatus'

    execution_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('jobMetadata.job_id'), nullable=False)
    start_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    end_datetime = Column(DateTime)
    status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)
    job = relationship('JobMetadata')





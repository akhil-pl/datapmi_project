from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Text, DECIMAL, Enum, func, JSON
from sqlalchemy.orm import relationship, mapper, clear_mappers
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


# Table to add source connection details by a user.
# Need to add user table ForeignKey once user table is created
class Connections(Base):
    __tablename__ = 'connections'

    # id = Column(Integer, autoincrement=True, primary_key=True)
    connection_id = Column(Integer, autoincrement=True, primary_key=True)
    connection_name = Column(String(255), nullable=False)
    source = Column(String(255), nullable=False)
    host = Column(String(255), nullable=False)
    user = Column(String(255), nullable=False)
    password = Column(String(255))
    port = Column(String(255), nullable=False)
    database = Column(String(255), nullable=False)
    






# Table to store Jobs Metadata
class JobMetadata(Base):
    __tablename__ = 'jobMetadata'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(255), nullable=False)
    job_type = Column(Enum('Ingestion', 'Transformation', 'Pipeline'), nullable=False) # Each curresponding table will have a job foreign to update the status
    creation_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(String(255), nullable=False)
    job_detail = Column(JSON, nullable=False)

    # Define one-to-many relationship with JobExecutionStatus
    job_execution_statuses = relationship('JobExecutionStatus', back_populates='job_metadata')    



class JobExecutionStatus(Base):
    __tablename__ = 'jobExecutionStatus'

    job_execution_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('jobMetadata.job_id'))
    start_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    end_datetime = Column(DateTime)
    status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)

    # Define the back-reference to JobMetadata
    job_metadata = relationship('JobMetadata', back_populates='job_execution_statuses')










class PipelineMetadata(Base):
    __tablename__ = 'pipelineMetadata'

    pipeline_id = Column(Integer, primary_key=True, autoincrement=True)
    job_execution_id = Column(Integer, ForeignKey('jobExecutionStatus.job_execution_id'), nullable=False)
    pipeline_detail = Column(JSON, nullable=False) # [{"Transformation":"Trans1 logic"}, {"Ingestion":"Ing1 logic"}, {"Transformation":"Trans2 logic"}]
    total_task_count = Column(Integer, nullable=False)
    current_running_task_number = Column(Integer, nullable=False) # To keep track and to start next task when a task is completed
    pipeline_start_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    pipeline_end_datetime = Column(DateTime)
    status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)

    # Define one-to-many relationship with PipelineExecutionStatus
    pipeline_execution_statuses = relationship('PipelineExecutionStatus', back_populates='pipeline_metadata')



# Table to store status of individual task in a pipeline
class PipelineExecutionStatus(Base):
    __tablename__ = 'pipelineExecutionStatus'

    pipeline_execution_id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(Integer, ForeignKey('pipelineMetadata.pipeline_id'), nullable=False)
    task_number = Column(Integer, nullable=False)
    task_type = Column(Enum('Transformation', 'Ingestion'), nullable=False)
    execution_start_datetime = Column(DateTime, nullable=False)
    execution_end_datetime = Column(DateTime)
    status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)

    # Define the back-reference to PipelineMetadata
    pipeline_metadata = relationship('PipelineMetadata', back_populates='pipeline_execution_statuses')

    









class TransformationMetadata(Base):
    __tablename__ = 'transformationMetadata'

    transformation_id = Column(Integer, primary_key=True, autoincrement=True)
    called_by = Column(Enum('Job', 'Pipeline'), nullable=False)
    transformation_start_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    transformation_end_datetime = Column(DateTime)
    status = Column(Enum('In Progress', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)
    transformation_detail = Column(JSON, nullable=False)



class TransformationJobPair(Base):
    __tablename__ = 'transformationJobPair'

    transformation_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), primary_key=True)
    job_execution_id = Column(Integer, ForeignKey('jobExecutionStatus.job_execution_id'), primary_key=True)



class TransformationPipelinePair(Base):
    __tablename__ = 'transformationPipelinePair'

    transformation_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), primary_key=True)
    pipeline_execution_id = Column(Integer, ForeignKey('pipelineExecutionStatus.pipeline_execution_id'), primary_key=True)

    









class IngestionMetadata(Base):
    __tablename__ = 'ingestionMetadata'

    ingestion_id = Column(Integer, primary_key=True, autoincrement=True)
    called_by = Column(Enum('Job', 'Pipeline'), nullable=False)
    ingestion_start_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    ingestion_end_datetime = Column(DateTime)
    status = Column(Enum('In Progress', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)
    ingestion_detail = Column(JSON, nullable=False)



class IngestionJobPair(Base):
    __tablename__ = 'ingestionJobPair'

    ingestion_id = Column(Integer, ForeignKey('ingestionMetadata.ingestion_id'), primary_key=True)
    job_execution_id = Column(Integer, ForeignKey('jobExecutionStatus.job_execution_id'), primary_key=True)



class IngestionPipelinePair(Base):
    __tablename__ = 'ingestionPipelinePair'

    ingestion_id = Column(Integer, ForeignKey('ingestionMetadata.ingestion_id'), primary_key=True)
    pipeline_execution_id = Column(Integer, ForeignKey('pipelineExecutionStatus.pipeline_execution_id'), primary_key=True)

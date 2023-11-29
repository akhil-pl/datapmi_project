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
    
    # Establishing one to one relationship with child tables
    jobDetails = relationship("JobDetails", backref="job", cascade="all, delete") # Cascading effect to delete all child relations
    jobExecutionStatus = relationship("JobExecutionStatus", backref="job", cascade="all, delete")

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'base_job'
    }

# Establishing relationship with job type tables
JobMetadata.transformation_job_pair = relationship('TransformationJobPair', uselist=False, back_populates='job_metadata', foreign_keys=[JobMetadata.job_id], viewonly=True)
JobMetadata.ingestion_job_pair = relationship('IngestionJobPair', uselist=False, back_populates='job_metadata', foreign_keys=[JobMetadata.job_id], viewonly=True)
JobMetadata.pipeline_job_pair = relationship('PipelineJobPair', uselist=False, back_populates='job_metadata', foreign_keys=[JobMetadata.job_id], viewonly=True)




# Table to store actual job details as Pythn dictionary with "Type":"Logic" pair in JSON form
class JobDetails(Base):
    __tablename__ = 'jobDetails'

    job_detail_id = Column(Integer, ForeignKey('jobMetadata.job_id'), primary_key=True, autoincrement=True)
    job_detail = Column(JSON, nullable=False) # "Type":"Logic" pair in JSON form
    job = relationship('JobMetadata')



class JobExecutionStatus(Base):
    __tablename__ = 'jobExecutionStatus'

    execution_id = Column(Integer, ForeignKey('jobMetadata.job_id'), primary_key=True, autoincrement=True)
    start_datetime = Column(DateTime)
    end_datetime = Column(DateTime)
    status = Column(Enum('NotStarted', 'Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)
    job = relationship('JobMetadata')







# Table to add pipeline metadata
class PipelineMetadata(Base):
    __tablename__ = 'pipelineMetadata'

    pipeline_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('jobMetadata.job_id'), nullable=False)
    pipeline_name = Column(String(255), nullable=False)
    total_task_count = Column(Integer, nullable=False)
    current_running_task_number = Column(Integer, nullable=False) # To keep track and to start next task when a task is completed
    pipeline_start_datetime = Column(DateTime, server_default=func.now(), nullable=False)
    pipeline_end_datetime = Column(DateTime)
    status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)

    job = relationship('JobMetadata', back_populates='pipelines')
    


# Table to store actual pipeline details as Python list having indivudual task as dictionary
class PipelineDetails(Base):
    __tablename__ = 'pipelineDetails'

    pipeline_detail_id = Column(Integer, ForeignKey('pipelineMetadata.pipeline_id'), primary_key=True, autoincrement=True)
    pipeline_detail = Column(JSON, nullable=False) # [{"Transformation":"Trans1 logic"}, {"Ingestion":"Ing1 logic"}, {"Transformation":"Trans2 logic"}]
    connection_id = Column(Integer, ForeignKey('connections.connection_id'), nullable=False)
    pipeline = relationship('pipelineMetadata')



# Table to store status of individual task in a pipeline
class PipelineExecutionStatus(Base):
    __tablename__ = 'pipelineExecution'

    execution_id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(Integer, ForeignKey('pipelineMetadata.pipeline_id'), nullable=False)
    task_number = Column(Integer, nullable=False)
    task_type = Column(Enum('Transformation', 'Ingestion'), nullable=False)
    execution_start_datetime = Column(DateTime, nullable=False)
    execution_end_datetime = Column(DateTime)
    status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)

    pipeline = relationship('PipelineMetadata')

    __mapper_args__ = {
        'polymorphic_on': task_type,
        'polymorphic_identity': 'base_task'
    }
    
PipelineExecutionStatus.transformation_pair = relationship('TransformationPipelinePair', uselist=False, back_populates='pipeline_execution', foreign_keys=[PipelineExecutionStatus.execution_id], viewonly=True)
PipelineExecutionStatus.ingestion_pair = relationship('IngestionPipelinePair', uselist=False, back_populates='pipeline_execution', foreign_keys=[PipelineExecutionStatus.execution_id], viewonly=True)



# Table to store job id foreign key  of each pipeline, so that the job status can be updated once the pipeline is completed or fails
class PipelineJobPair(Base):
    __tablename__ = 'pipelineJobPair'

    pipeline_execution_id = Column(Integer, ForeignKey('pipelineExecution.execution_id'), primary_key=True)
    job_metadata_id = Column(Integer, ForeignKey('jobMetadata.job_id'), primary_key=True)

    pipeline_execution = relationship('PipelineExecutionStatus', back_populates='ingestion_pair')
    job_metadata = relationship('JobMetadata', back_populates='pipeline_job_pair')









class IngestionMetadata(Base):
    __tablename__ = 'ingestionMetadata'

    ingestion_id = Column(Integer, primary_key=True, autoincrement=True)
    called_by = Column(Enum('Job', 'Pipeline'))
    ingestion_name = Column(String(255), nullable=False)
    ingestion_start_datetime = Column(DateTime, nullable=False)
    ingestion_end_datetime = Column(DateTime)
    status = Column(Enum('Running', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)


    __mapper_args__ = {
        'polymorphic_on': called_by,
        'polymorphic_identity': 'base_ingestion'
    }

IngestionMetadata.ingestion_pipeline_pair = relationship('IngestionPipelinePair', uselist=False, back_populates='ingestion_metadata', foreign_keys=[IngestionMetadata.ingestion_id], viewonly=True)
IngestionMetadata.ingestion_job_pair = relationship('IngestionJobPair', uselist=False, back_populates='ingestion_metadata', foreign_keys=[IngestionMetadata.ingestion_id], viewonly=True)



# Table to store Pipeline id if the injestion is called by a pipeline
class IngestionPipelinePair(Base):
    __tablename__ = 'ingestionPipelinePair'

    ingestion_metadata_id = Column(Integer, ForeignKey('ingestionMetadata.ingestion_id'), primary_key=True)
    pipeline_execution_id = Column(Integer, ForeignKey('pipelineExecution.execution_id'), primary_key=True)
    
    ingestion_metadata = relationship('IngestionMetadata', back_populates='ingestion_pipeline_pair')
    pipeline_execution = relationship('PipelineExecutionStatus', back_populates='ingestion_pair')


# Table to store Job id if the injestion is called by a Job
class IngestionJobPair(Base):
    __tablename__ = 'ingestionJobPair'

    ingestion_metadata_id = Column(Integer, ForeignKey('ingestionMetadata.ingestion_id'), primary_key=True)
    job_metadata_id = Column(Integer, ForeignKey('jobMetadata.job_id'), primary_key=True)

    ingestion_metadata = relationship('IngestionMetadata', back_populates='ingestion_job_pair')
    job_metadata = relationship('JobMetadata', back_populates='ingestion_job_pair')


class IngestionDetails(Base):
    __tablename__ = 'ingestionDetails'

    ingestion_detail_id = Column(Integer, ForeignKey('ingestionMetadata.ingestion_id'), primary_key=True, autoincrement=True)
    ingestion_detail = Column(JSON, nullable=False) # [{"Transformation":"Trans1 logic"}, {"Ingestion":"Ing1 logic"}, {"Transformation":"Trans2 logic"}]
    connection_id = Column(Integer, ForeignKey('connections.connection_id'), nullable=False)
    ingestion = relationship('ingestionMetadata')










class TransformationMetadata(Base):
    __tablename__ = 'transformationMetadata'

    transformation_id = Column(Integer, primary_key=True, autoincrement=True)
    called_by = Column(Enum('Job', 'Pipeline', 'Ingestion'))
    transformation_start_datetime = Column(DateTime, nullable=False)
    transformation_end_datetime = Column(DateTime)
    status = Column(Enum('In Progress', 'Completed', 'Failed'), nullable=False)
    error_message = Column(Text)

    __mapper_args__ = {
        'polymorphic_on': called_by,
        'polymorphic_identity': 'base_transformation'
    }

TransformationMetadata.transformation_job_pair = relationship('TransformationJobPair', uselist=False, back_populates='transformation_metadata', foreign_keys=[TransformationMetadata.transformation_id], viewonly=True)
TransformationMetadata.transformation_pipeline_pair = relationship('TransformationPipelinePair', uselist=False, back_populates='transformation_metadata', foreign_keys=[TransformationMetadata.transformation_id], viewonly=True)
TransformationMetadata.transformation_ingestion_pair = relationship('TransformationIngestionPair', uselist=False, back_populates='transformation_metadata', foreign_keys=[TransformationMetadata.transformation_id], viewonly=True)




class MetricsMetadata(Base):
    __tablename__ = 'metricsMetadata'

    metric_id = Column(Integer, primary_key=True, autoincrement=True)
    transformation_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), nullable=False)
    metric_name = Column(String(255), nullable=False)
    metric_value = Column(DECIMAL(10, 2), nullable=False)
    transformation = relationship('TransformationMetadata')














# Table to store Job id if the transformation is called by a Job
class TransformationJobPair(Base):
    __tablename__ = 'transformationJobPair'

    transformation_metadata_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), primary_key=True)
    job_metadata_id = Column(Integer, ForeignKey('jobMetadata.job_id'), primary_key=True)

    transformation_metadata = relationship('TransformationMetadata', back_populates='transformation_job_pair')
    job_metadata = relationship('JobMetadata', back_populates='transformation_job_pair')


# Table to store Pipeline id if the transformation is called by a pipeline
class TransformationPipelinePair(Base):
    __tablename__ = 'transformationPipelinePair'

    transformation_metadata_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), primary_key=True)
    pipeline_execution_id = Column(Integer, ForeignKey('pipelineExecution.execution_id'), primary_key=True)

    transformation_metadata = relationship('TransformationMetadata', back_populates='transformation_pipeline_pair')
    pipeline_execution = relationship('PipelineExecutionStatus', back_populates='transformation_pair')


# Table to store ingestion id if the transformation is called by a ingestion
class TransformationIngestionPair(Base):
    __tablename__ = 'transformationIngestionPair'

    transformation_metadata_id = Column(Integer, ForeignKey('transformationMetadata.transformation_id'), primary_key=True)
    ingestion_metadata_id = Column(Integer, ForeignKey('ingestionMetadata.ingestion_id'), primary_key=True)
    

    transformation_metadata = relationship('TransformationMetadata', back_populates='transformation_ingestion_pair')


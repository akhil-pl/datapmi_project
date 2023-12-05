from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload


from data.database import get_db
from data.model import (
    PipelineMetadata, 
    PipelineExecutionStatus, 
    TransformationPipelinePair, 
    TransformationMetadata,
    IngestionPipelinePair,
    IngestionMetadata
    )

router = APIRouter()




@router.get("/jobs/pipelines/{job_execution_id}", tags=["pipeline"])
def read_pipeline(job_execution_id: int, db: Session = Depends(get_db)):
    """
    Get jobs, details and its execution status.
    """
    pipeline_data = (
        db.query(PipelineMetadata)
        .options(joinedload(PipelineMetadata.pipeline_execution_statuses))
        .filter(PipelineMetadata.job_execution_id == job_execution_id)
        .first()
    )

    if not pipeline_data:
        raise HTTPException(status_code=404, detail="No pipeline with that execution id")

    # Convert the SQLAlchemy model to a dictionary for serialization
    pipeline_dict = pipeline_data.__dict__
    # Remove the "_sa_instance_state" key, which is not JSON serializable
    pipeline_dict.pop("_sa_instance_state", None)

    return pipeline_dict









@router.get("/pipelines/{pipeline_id}", tags=["pipeline"])
def read_pipeline(pipeline_id: int, db: Session = Depends(get_db)):
    """
    Get jobs, details and its execution status.
    """
    pipeline_data = (
        db.query(PipelineMetadata)
        .options(joinedload(PipelineMetadata.pipeline_execution_statuses))
        .filter(PipelineMetadata.pipeline_id == pipeline_id)
        .first()
    )

    if not pipeline_data:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Convert the SQLAlchemy model to a dictionary for serialization
    pipeline_dict = pipeline_data.__dict__
    # Remove the "_sa_instance_state" key, which is not JSON serializable
    pipeline_dict.pop("_sa_instance_state", None)

    return pipeline_dict







@router.get("/pipelines/", tags=["pipeline"])
def read_all_jobs_latest_status(db: Session = Depends(get_db)):
    """
    Get all jobs with the status of the latest execution.
    """
    pipelines = db.query(PipelineMetadata).all()

    return pipelines









@router.get("/pipeline/execution/{pipeline_execution_id}", tags=["pipeline"])
def read_pipeline(pipeline_execution_id: int, db: Session = Depends(get_db)):
    """
    Get ingestion, details and its execution status.
    """
    pipeline_execution = db.query(PipelineExecutionStatus).filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_execution_id).first()

    if not pipeline_execution:
        raise HTTPException(status_code=404, detail="Pipeline Execution not found")
    
    task_type = pipeline_execution.task_type
    if task_type == 'Transformation':
        transformation_pair = db.query(TransformationPipelinePair).filter(TransformationPipelinePair.pipeline_execution_id == pipeline_execution.pipeline_execution_id).first()
        task = db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_pair.transformation_id).first()
    elif task_type == 'Ingestion':
        ingestion_pair = db.query(IngestionPipelinePair).filter(IngestionPipelinePair.pipeline_execution_id == pipeline_execution.pipeline_execution_id).first()
        task = db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_pair.ingestion_id).first()
    
    return {"Execution": pipeline_execution, "Task": task}




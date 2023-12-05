from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload


from data.database import get_db
from data.model import JobExecutionStatus, TransformationMetadata, TransformationJobPair, TransformationPipelinePair, PipelineExecutionStatus, PipelineMetadata

router = APIRouter()



@router.get("/jobs/transformations/{job_execution_id}", tags=["transformation"])
def read_pipeline(job_execution_id: int, db: Session = Depends(get_db)):
    """
    Get ingestion, details and its execution status.
    """
    transformation_job_pair = db.query(TransformationJobPair).filter(TransformationJobPair.job_execution_id == job_execution_id).first()
    if not transformation_job_pair:
        raise HTTPException(status_code=404, detail="No Transformation job found with that execution ID")
    
    transformation = db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_job_pair.transformation_id).first()

    if not transformation:
        raise HTTPException(status_code=404, detail="Transformation not found")
    
    called_by = transformation.called_by
    if called_by == 'Job':
        called_source = (
            db.query(JobExecutionStatus)
            .options(joinedload(JobExecutionStatus.job_metadata))
            .filter(JobExecutionStatus.job_execution_id == job_execution_id)
            .first()
        )
    else:
        called_source = []

    return {"Transformation": transformation, "called_from": called_source}








@router.get("/pipelines/transformations/{pipeline_execution_id}", tags=["transformation"])
def read_pipeline(pipeline_execution_id: int, db: Session = Depends(get_db)):
    """
    Get ingestion, details and its execution status.
    """
    transformation_pipeline_pair = db.query(TransformationPipelinePair).filter(TransformationPipelinePair.pipeline_execution_id == pipeline_execution_id).first()
    if not transformation_pipeline_pair:
        raise HTTPException(status_code=404, detail="No Transformation Task found with that execution ID")
    
    transformation = db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_pipeline_pair.transformation_id).first()

    if not transformation:
        raise HTTPException(status_code=404, detail="Transformation not found")
    
    called_by = transformation.called_by
    if called_by == 'Pipeline':
        called_source = (
            db.query(PipelineExecutionStatus)
            .options(joinedload(PipelineExecutionStatus.pipeline_metadata))
            .filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_execution_id)
            .first()
        )
    else:
        called_source = []

    return {"Transformation": transformation, "called_from": called_source}









@router.get("/transformations/{transformation_id}", tags=["transformation"])
def read_pipeline(transformation_id: int, db: Session = Depends(get_db)):
    """
    Get ingestion, details and its execution status.
    """
    transformation = db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_id).first()

    if not transformation:
        raise HTTPException(status_code=404, detail="Transformation not found")
    
    called_by = transformation.called_by
    if called_by == 'Job':
        job_pair = db.query(TransformationJobPair).filter(TransformationJobPair.transformation_id == transformation.transformation_id).first()
        called_source = (
            db.query(JobExecutionStatus)
            .options(joinedload(JobExecutionStatus.job_metadata))
            .filter(JobExecutionStatus.job_execution_id == job_pair.job_execution_id)
            .first()
        )
    elif called_by == 'Pipeline':
        pipeline_pair = db.query(TransformationPipelinePair).filter(TransformationPipelinePair.transformation_id == transformation.transformation_id).first()
        called_source = (
            db.query(PipelineExecutionStatus)
            .options(joinedload(PipelineExecutionStatus.pipeline_metadata))
            .filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_pair.pipeline_execution_id)
            .first()
        )
    else:
        called_source = []

    return {"Transformation": transformation, "called_from": called_source}







@router.get("/transformations/", tags=["transformation"])
def read_all_jobs_latest_status(db: Session = Depends(get_db)):
    """
    Get all ingestions with the status of execution.
    """
    transformations = db.query(TransformationMetadata).all()

    transformations_list = {'Job': [], 'Pipeline': []}
    for transformation in transformations:
        transformations_list[transformation.called_by].append(transformation)

    return transformations_list




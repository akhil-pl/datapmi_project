from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload


from data.database import get_db
from data.model import JobExecutionStatus, TransformationMetadata, TransformationJobPair

router = APIRouter()



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
    else:
        called_source = []

    return {"Transformation": transformation, "called_from": called_source}







@router.get("/transformations/", tags=["transformation"])
def read_all_jobs_latest_status(db: Session = Depends(get_db)):
    """
    Get all ingestions with the status of execution.
    """
    transformations = db.query(TransformationMetadata).all()

    return transformations




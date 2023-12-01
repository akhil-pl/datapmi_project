from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload


from data.database import get_db
from data.model import JobExecutionStatus, IngestionMetadata, IngestionJobPair

router = APIRouter()



@router.get("/ingestions/{ingestion_id}", tags=["ingestion"])
def read_pipeline(ingestion_id: int, db: Session = Depends(get_db)):
    """
    Get ingestion, details and its execution status.
    """
    ingestion = db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_id).first()

    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")
    
    called_by = ingestion.called_by
    if called_by == 'Job':
        job_pair = db.query(IngestionJobPair).filter(IngestionJobPair.ingestion_id == ingestion.ingestion_id).first()
        called_source = (
            db.query(JobExecutionStatus)
            .options(joinedload(JobExecutionStatus.job_metadata))
            .filter(JobExecutionStatus.job_execution_id == job_pair.job_execution_id)
            .first()
        )
    else:
        called_source = []

    return {"Transformation": ingestion, "called_from": called_source}







@router.get("/ingestions/", tags=["ingestion"])
def read_all_jobs_latest_status(db: Session = Depends(get_db)):
    """
    Get all ingestions with the status.
    """
    ingestions = db.query(IngestionMetadata).all()

    return ingestions




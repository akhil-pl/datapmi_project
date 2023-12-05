from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload


from data.database import get_db
from data.model import JobExecutionStatus, IngestionMetadata, IngestionJobPair, IngestionPipelinePair, PipelineExecutionStatus, PipelineMetadata

router = APIRouter()



@router.get("/jobs/ingestions/{job_execution_id}", tags=["ingestion"])
def read_pipeline(job_execution_id: int, db: Session = Depends(get_db)):
    """
    Get ingestion, details and its execution status.
    """
    ingestion_job_pair = db.query(IngestionJobPair).filter(IngestionJobPair.job_execution_id == job_execution_id).first()
    if not ingestion_job_pair:
        raise HTTPException(status_code=404, detail="No Ingestion job found with that execution ID")
    
    ingestion = db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_job_pair.ingestion_id).first()

    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")
    
    called_by = ingestion.called_by
    if called_by == 'Job':
        called_source = (
            db.query(JobExecutionStatus)
            .options(joinedload(JobExecutionStatus.job_metadata))
            .filter(JobExecutionStatus.job_execution_id == job_execution_id)
            .first()
        )
    else:
        called_source = []

    return {"Ingestion": ingestion, "called_from": called_source}







@router.get("/pipelines/ingestions/{pipeline_execution_id}", tags=["ingestion"])
def read_pipeline(pipeline_execution_id: int, db: Session = Depends(get_db)):
    """
    Get ingestion, details and its execution status.
    """
    ingestion_pipeline_pair = db.query(IngestionPipelinePair).filter(IngestionPipelinePair.pipeline_execution_id == pipeline_execution_id).first()
    if not ingestion_pipeline_pair:
        raise HTTPException(status_code=404, detail="No Ingestion task found with that execution ID")
    
    ingestion = db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_pipeline_pair.ingestion_id).first()

    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")
    
    called_by = ingestion.called_by
    if called_by == 'Pipeline':
        called_source = (
            db.query(PipelineExecutionStatus)
            .options(joinedload(PipelineExecutionStatus.pipeline_metadata))
            .filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_execution_id)
            .first()
        )
    else:
        called_source = []

    return {"Ingestion": ingestion, "called_from": called_source}









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
    elif called_by == 'Pipeline':
        pipeline_pair = db.query(IngestionPipelinePair).filter(IngestionPipelinePair.ingestion_id == ingestion.ingestion_id).first()
        called_source = (
            db.query(PipelineExecutionStatus)
            .options(joinedload(PipelineExecutionStatus.pipeline_metadata))
            .filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_pair.pipeline_execution_id)
            .first()
        )
    else:
        called_source = []

    return {"Ingestion": ingestion, "called_from": called_source}







@router.get("/ingestions/", tags=["ingestion"])
def read_all_jobs_latest_status(db: Session = Depends(get_db)):
    """
    Get all ingestions with the status.
    """
    ingestions = db.query(IngestionMetadata).all()

    ingestions_list = {'Job': [], 'Pipeline': []}
    for ingestion in ingestions:
        ingestions_list[ingestion.called_by].append(ingestion)

    return ingestions_list




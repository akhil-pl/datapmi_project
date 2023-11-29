from fastapi import APIRouter, Depends, Query, HTTPException
from enum import Enum
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
from typing import List


from data.database import get_db
# from data.model import PipelineMetadata, PipelineExecution

router = APIRouter()


# Pydantic models for data validation
class SupportedJobType(str, Enum):
    Ingestion = "Ingestion"
    Transformation = "Transformation"
    Pipeline = "Pipeline"

class JobMetadateBase(BaseModel):
    job_name: str
    job_type: SupportedJobType
    created_by: str

    class Config: # To response model to work properly
        orm_mode = True

class JobMetadateModel(JobMetadateBase):
    job_id: int
    creation_datetime: datetime



# API to add new jobs
@router.get("/jobs/", response_model=List[JobMetadateModel], tags=["jobs"])
def list_jobs(skip: int = 0, limit: int = 10, db : Session = Depends(get_db)):
    """
    Get all the jobs.
    """
    jobs = db.query(JobMetadata).offset(skip).limit(limit).all()
    db.close()
    return jobs




@router.post("/jobs/", response_model=JobMetadateModel, tags=["jobs"])
def create_job(new_job: JobMetadateBase, db : Session = Depends(get_db)):
    """
    Create a new job.
    """
    db_job = JobMetadata(**new_job.dict())
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    db.close()
    return db_job



@router.get("/jobs/{job_id}", tags=["jobs"])
def read_pipeline(job_id: int, db : Session = Depends(get_db)):
    """
    Get jobs and its execution status.
    """
    job = db.query(JobMetadata).filter(JobMetadata.job_id == job_id).first()
    db.close()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    job_status = db.query(JobExecutionStatus).filter(JobExecutionStatus.job_id == job_id).all()
    return {"job": job, "job execution status": job_status}





@router.post("/jobs/{job_id}/start/", tags=["dummies"])
def start_jobs(job_id: int, db : Session = Depends(get_db)):
    """
    For manually setting a job execution as start.
    """
    job = db.query(JobMetadata).filter(JobMetadata.job_id == job_id).first()
    if job is None:
        db.close()
        raise HTTPException(status_code=404, detail="Job not found")
    status = "Running"
    db_status = JobExecutionStatus(job_id=job_id, status=status)
    db.add(db_status)
    db.commit()
    db.refresh(db_status)
    db.close()
    return db_status





@router.patch("/jobs/{job_ex_id}/failed/", tags=["dummies"])
def fail_jobs(job_ex_id: int, error_message: str, db : Session = Depends(get_db)):
    """
    For manually setting a job execution as failed with error.
    """
    job_stat = db.query(JobExecutionStatus).filter(JobExecutionStatus.execution_id == job_ex_id).first()
    if job_stat is None:
        db.close()
        raise HTTPException(status_code=404, detail="Job execution not found")
    # status = "Running"
    db.query(JobExecutionStatus).filter(JobExecutionStatus.execution_id == job_ex_id).update({
        "status": "Failed",
        "end_datetime": datetime.utcnow(),
        "error_message":error_message
    })
    db.commit()
    db.close()
    return {"message": "Job execution status and end datetime updated successfully"}







@router.patch("/jobs/{job_ex_id}/completed/", tags=["dummies"])
def end_jobs(job_ex_id: int, db : Session = Depends(get_db)):
    """
    For manually setting a job execution as complete.
    """
    job_stat = db.query(JobExecutionStatus).filter(JobExecutionStatus.execution_id == job_ex_id).first()
    if job_stat is None:
        db.close()
        raise HTTPException(status_code=404, detail="Job execution not found")
    # status = "Running"
    db.query(JobExecutionStatus).filter(JobExecutionStatus.execution_id == job_ex_id).update({
        "status": "Completed",
        "end_datetime": datetime.utcnow()
    })
    db.commit()
    db.close()
    return {"message": "Job execution status and end datetime updated successfully"}








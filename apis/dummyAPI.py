from fastapi import APIRouter, Depends, Query, HTTPException
from enum import Enum
from sqlalchemy import create_engine, func
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import Session, joinedload
from pydantic import BaseModel
from datetime import datetime
from typing import List


from data.database import get_db
from data.model import JobMetadata, JobExecutionStatus, TransformationMetadata, TransformationJobPair

router = APIRouter()







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
    job_status = db.query(JobExecutionStatus).filter(JobExecutionStatus.job_id == job_id, JobExecutionStatus.status == status).first()
    if job_status:
        db.close()
        raise HTTPException(status_code=403, detail="Job already running")
    db_status = JobExecutionStatus(job_id=job_id, status=status)
    db.add(db_status)
    db.commit()
    db.refresh(db_status)

    job_type = job.job_type
    if job_type == 'Transformation':
        db_transformation = TransformationMetadata(called_by="Job", status="In Progress", transformation_detail=job.job_detail)
        db.add(db_transformation)
        db.commit()
        db.refresh(db_transformation)

        db_transJobPair = TransformationJobPair(transformation_id=db_transformation.transformation_id, job_execution_id=db_status.job_execution_id)
        db.add(db_transJobPair)
        db.commit()
        db.refresh(db_transJobPair)
    
    else:
        db_status = "Job Type not supported"

    return {"Message": "Job exicution started", "Execution Info": db_status}





@router.patch("/jobs/{job_id}/failed/", tags=["dummies"])
def fail_jobs(job_id: int, error_message: str, db : Session = Depends(get_db)):
    """
    For manually setting a job execution as failed with error.
    """
    job = db.query(JobMetadata).filter(JobMetadata.job_id == job_id).first()
    if job is None:
        db.close()
        raise HTTPException(status_code=404, detail="Job not found")
    job_status = db.query(JobExecutionStatus).filter(JobExecutionStatus.job_id == job_id, JobExecutionStatus.status == "Running").first()
    print("Job model of execution", job_status)
    if job_status is None:
        db.close()
        raise HTTPException(status_code=404, detail="Job is not currently running")
    job_ex_id = job_status.job_execution_id
    db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == job_ex_id).update({
        "status": "Failed",
        "end_datetime": datetime.utcnow(),
        "error_message":error_message
    })
    db.commit()
    db.close()
    return {"message": "Job execution status and end datetime updated successfully"}







@router.patch("/jobs/{job_id}/completed/", tags=["dummies"])
def end_jobs(job_id: int, db : Session = Depends(get_db)):
    """
    For manually setting a job execution as complete.
    """
    job = db.query(JobMetadata).filter(JobMetadata.job_id == job_id).first()
    if job is None:
        db.close()
        raise HTTPException(status_code=404, detail="Job not found")
    job_status = db.query(JobExecutionStatus).filter(JobExecutionStatus.job_id == job_id, JobExecutionStatus.status == "Running").first()
    print("Job model of execution", job_status)
    if job_status is None:
        db.close()
        raise HTTPException(status_code=404, detail="Job is not currently running")
    job_ex_id = job_status.job_execution_id
    db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == job_ex_id).update({
        "status": "Completed",
        "end_datetime": datetime.utcnow()
    })
    db.commit()
    db.close()
    return {"message": "Job execution status and end datetime updated successfully"}








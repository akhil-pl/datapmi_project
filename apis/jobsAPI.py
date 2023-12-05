from fastapi import APIRouter, Depends, HTTPException
from enum import Enum
from sqlalchemy.orm import Session, joinedload
from pydantic import BaseModel
from datetime import datetime


from data.database import get_db
from data.model import JobMetadata

router = APIRouter()


# Pydantic models for data validation
class SupportedJobType(str, Enum):
    Transformation = "Transformation"
    Ingestion = "Ingestion"
    Pipeline = "Pipeline"

class JobMetadateBase(BaseModel):
    job_name: str
    job_type: SupportedJobType
    created_by: str
    job_detail: dict

    class Config: # To response model to work properly
        orm_mode = True

class JobMetadateModel(JobMetadateBase):
    job_id: int
    creation_datetime: datetime


class AllowedJobStatus(str, Enum):
    Running = "Running"
    Completed = "Completed"
    Failed = "Failed"

class JobExecutionStatusModel(BaseModel):
    job_execution_id: int
    job_id: int
    start_datetime: datetime
    end_datetime: datetime
    status: AllowedJobStatus
    error_message: str

    class Config:
        orm_mode = True


class FullJobDetailsModel(BaseModel):
    job: JobMetadateModel
    job_execution_status: JobExecutionStatusModel

    class Config:
        orm_mode = True



# API to add new jobs
@router.post("/jobs/", response_model=JobMetadateModel, tags=["jobs"])
def create_job(new_job: JobMetadateBase, db: Session = Depends(get_db)):
    """
    Create a new job. job_detail to be given as a dictionary with job_type as key (Pipeline / Transformation / Ingession) and logic as value. Find the following example.
    > For pipeline jobs:-   {"Pipeline" : [{ "Ingestion" : "Pipeline Ingestion 1 logic" }, { "Transformation" : "Pipeline Transformation1 logic" }, { "Ingestion" : "Pipeline Ingestion 2 logic" }, { "Transformation" : "Pipeline Transformation 2 logic" }]}
    
    > For ingestion jobs:-   { "Ingestion" : "Ingestion logic" }

    > For transformation jobs:-   { "Transformation" : "Transformation logic" }
    """
    try:
        # Create JobMetadata record
        db_job = JobMetadata(**new_job.dict())
        db.add(db_job)
        db.commit()
        db.refresh(db_job)

        return db_job

    except Exception as e:
        # Rollback the transaction in case of any exception
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")











@router.get("/jobs/{job_id}", tags=["jobs"])
def read_pipeline(job_id: int, db: Session = Depends(get_db)):
    """
    Get jobs, details and its execution status.
    """
    job_data = (
        db.query(JobMetadata)
        .options(joinedload(JobMetadata.job_execution_statuses))
        .filter(JobMetadata.job_id == job_id)
        .first()
    )

    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")

    # Convert the SQLAlchemy model to a dictionary for serialization
    job_dict = job_data.__dict__
    # Remove the "_sa_instance_state" key, which is not JSON serializable
    job_dict.pop("_sa_instance_state", None)

    return job_dict







@router.get("/jobs/", tags=["jobs"])
def read_all_jobs_latest_status(db: Session = Depends(get_db)):
    """
    Get all jobs with the status of the latest execution.
    """
    job_data = (
        db.query(JobMetadata)
        .options(joinedload(JobMetadata.job_execution_statuses))
        .all()
    )

    # Convert the SQLAlchemy models to a list of dictionaries for serialization
    jobs_list = {'Pipeline': [], 'Transformation': [], 'Ingestion': []}
    for job in job_data:
        job_dict = {
            'job_id': job.job_id,
            'job_name': job.job_name,
            'job_type': job.job_type,
            'last_execution_status': {},
        }

        if job.job_execution_statuses:
            latest_execution_status = job.job_execution_statuses[0]  # Assuming the list is ordered by execution_id
            job_dict['last_execution_status'] = {
                'job_execution_id': latest_execution_status.job_execution_id,
                'start_datetime': latest_execution_status.start_datetime if latest_execution_status.start_datetime else None,
                'end_datetime': latest_execution_status.end_datetime if latest_execution_status.end_datetime else None,
                'status': latest_execution_status.status if latest_execution_status.status else None,
            }

        jobs_list[job.job_type].append(job_dict)

    return jobs_list




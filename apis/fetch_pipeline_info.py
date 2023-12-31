from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy.orm import Session
from typing import List

from data.database import get_db
from data.model import Pipeline, PipelineStatus


router = APIRouter()


# Pydantic models for data validation
class PipelineBase(BaseModel):
    name: str
    description: str

    class Config: # To response model to work properly
        orm_mode = True

class PipelineCreate(PipelineBase):
    pass

class PipelineModel(PipelineBase):
    id: int
    created_at: datetime

class PipelineStatusBase(BaseModel):
    status: str

    class Config: # To response model to work properly
        orm_mode = True

class PipelineStatusCreate(PipelineStatusBase):
    pass

class PipelineStatusModel(PipelineStatusBase):
    id: int
    pipeline_id: int
    created_at: datetime

# API endpoints to create, read, and list pipelines and their statuses
@router.get("/pipelines/", response_model=List[PipelineModel], tags=["pipeline"])
def list_pipelines(skip: int = 0, limit: int = 10, db : Session = Depends(get_db)):
    pipelines = db.query(Pipeline).offset(skip).limit(limit).all()
    db.close()
    return pipelines







@router.post("/pipelines/", response_model=PipelineModel, tags=["pipeline"])
def create_pipeline(pipeline: PipelineCreate, db : Session = Depends(get_db)):
    db_pipeline = Pipeline(**pipeline.dict())
    db.add(db_pipeline)
    db.commit()
    db.refresh(db_pipeline)
    db.close()
    return db_pipeline






@router.get("/pipelines/{pipeline_id}", response_model=PipelineModel, tags=["pipeline"])
def read_pipeline(pipeline_id: int, db : Session = Depends(get_db)):
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    db.close()
    if pipeline is None:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline






@router.post("/pipelines/{pipeline_id}/status/", response_model=PipelineStatusModel, tags=["pipeline"])
def create_pipeline_status(pipeline_id: int, status: PipelineStatusCreate, db : Session = Depends(get_db)):
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if pipeline is None:
        db.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")
    db_status = PipelineStatus(pipeline_id=pipeline_id, **status.dict())
    db.add(db_status)
    db.commit()
    db.refresh(db_status)
    db.close()
    return db_status






@router.get("/pipelines/{pipeline_id}/status/", response_model=List[PipelineStatusModel], tags=["pipeline"])
def list_pipeline_statuses(pipeline_id: int, skip: int = 0, limit: int = 20, db : Session = Depends(get_db)):
    # db = SessionLocal()
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if pipeline is None:
        db.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")
    statuses = db.query(PipelineStatus).filter(PipelineStatus.pipeline_id == pipeline_id).offset(skip).limit(limit).all()
    db.close()
    return statuses

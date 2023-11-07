from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from data.database import get_db
from data.model import Pipeline, PipelineStatus

router = APIRouter()

@router.get("/dashboard/active_pipelines", tags=["dashboard"])
def active_pipelines(db : Session = Depends(get_db)):
    # db = SessionLocal()
    active_pipelines = db.query(Pipeline).join(Pipeline.statuses).filter(PipelineStatus.created_at > (datetime.now() - timedelta(days=7))).distinct().count()
    db.close()
    return {"active_pipelines": active_pipelines}

@router.get("/dashboard/pipeline_trends", tags=["dashboard"])
def pipeline_trends(db : Session = Depends(get_db)):
    # db = SessionLocal()
    trends_7days = db.query(PipelineStatus.created_at, func.count(PipelineStatus.id)).filter(PipelineStatus.created_at > (datetime.now() - timedelta(days=7))).group_by(PipelineStatus.created_at).all()
    trends_15days = db.query(PipelineStatus.created_at, func.count(PipelineStatus.id)).filter(PipelineStatus.created_at > (datetime.now() - timedelta(days=15))).group_by(PipelineStatus.created_at).all()
    trends_30days = db.query(PipelineStatus.created_at, func.count(PipelineStatus.id)).filter(PipelineStatus.created_at > (datetime.now() - timedelta(days=30))).group_by(PipelineStatus.created_at).all()
    trends_all = db.query(PipelineStatus.created_at, func.count(PipelineStatus.id)).group_by(PipelineStatus.created_at).all()
    db.close()
    return {
        "trends_7days": [{"date": str(date), "count": count} for date, count in trends_7days],
        "trends_15days": [{"date": str(date), "count": count} for date, count in trends_15days],
        "trends_30days": [{"date": str(date), "count": count} for date, count in trends_30days],
        "trends_all": [{"date": str(date), "count": count} for date, count in trends_all]
    }

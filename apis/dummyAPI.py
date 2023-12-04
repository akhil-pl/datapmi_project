from fastapi import APIRouter, Depends, Query, HTTPException
from enum import Enum
from sqlalchemy import create_engine, func
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import Session, joinedload
from pydantic import BaseModel
from datetime import datetime
from typing import List


from data.database import get_db
from data.model import (
    JobMetadata,
    JobExecutionStatus,
    PipelineMetadata,
    PipelineExecutionStatus,
    TransformationMetadata, 
    TransformationJobPair,
    TransformationPipelinePair,
    IngestionMetadata, 
    IngestionJobPair,
    IngestionPipelinePair
    )

router = APIRouter()




def update_pipeline_execution_status(pipeline_execution_id: int,
                                     status: str,
                                     db: Session,
                                     error_message: str=None):
    print(type(db))
    if status == 'Failed':
        db.query(PipelineExecutionStatus).filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_execution_id).update({
            "status": status,
            "execution_end_datetime": datetime.utcnow(),
            "error_message": "Task fails with message: < " + error_message + " >"
        })
        db.commit()
        
        pipeline_execution = db.query(PipelineExecutionStatus).filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_execution_id).first()
        db.query(PipelineMetadata).filter(PipelineMetadata.pipeline_id == pipeline_execution.pipeline_id).update({
            "status": status,
            "pipeline_end_datetime": datetime.utcnow(),
            "error_message": "Task number " + str(pipeline_execution.task_number) + " fails with message: < " + error_message + " >"
        })
        db.commit()

        pipeline = db.query(PipelineMetadata).filter(PipelineMetadata.pipeline_id == pipeline_execution.pipeline_id).first()
        db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == pipeline.job_execution_id).update({
            "status": status,
            "end_datetime": datetime.utcnow(),
            "error_message": "Pipeline " + pipeline.error_message
        })
        db.commit()

    elif status == 'Completed':
        db.query(PipelineExecutionStatus).filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_execution_id).update({
            "status": status,
            "execution_end_datetime": datetime.utcnow()
        })
        db.commit()

        pipeline_execution = db.query(PipelineExecutionStatus).filter(PipelineExecutionStatus.pipeline_execution_id == pipeline_execution_id).first()
        pipeline = db.query(PipelineMetadata).filter(PipelineMetadata.pipeline_id == pipeline_execution.pipeline_id).first()

        if pipeline.total_task_count != pipeline.current_running_task_number:
            db.query(PipelineMetadata).filter(PipelineMetadata.pipeline_id == pipeline_execution.pipeline_id).update({
                "current_running_task_number": pipeline.current_running_task_number + 1
            })
            db.commit()

            pipeline_list = pipeline.pipeline_detail
            next_task = pipeline_list[pipeline.current_running_task_number-1]
            next_task_type = list(next_task.keys())[0]
            db_pipeline_execution = PipelineExecutionStatus(pipeline_id=pipeline.pipeline_id, task_number=pipeline.current_running_task_number,
                                                            task_type=next_task_type, status="Running")
            db.add(db_pipeline_execution)
            db.commit()
            db.refresh(db_pipeline_execution)

            if next_task_type == 'Transformation':
                db_pipeline_transformation = TransformationMetadata(called_by="Pipeline", status="In Progress", transformation_detail=next_task["Transformation"])
                db.add(db_pipeline_transformation)
                db.commit()
                db.refresh(db_pipeline_transformation)

                db_transPipePair = TransformationPipelinePair(transformation_id=db_pipeline_transformation.transformation_id, pipeline_execution_id=db_pipeline_execution.pipeline_execution_id)
                db.add(db_transPipePair)
                db.commit()
                db.refresh(db_transPipePair)

            elif next_task_type == 'Ingestion':
                db_pipeline_ingestion = IngestionMetadata(called_by="Pipeline", status="In Progress", ingestion_detail=next_task["Ingestion"])
                db.add(db_pipeline_ingestion)
                db.commit()
                db.refresh(db_pipeline_ingestion)

                db_ingestPipePair = IngestionPipelinePair(ingestion_id=db_pipeline_ingestion.ingestion_id, pipeline_execution_id=db_pipeline_execution.pipeline_execution_id)
                db.add(db_ingestPipePair)
                db.commit()
                db.refresh(db_ingestPipePair)

        else:
            db.query(PipelineMetadata).filter(PipelineMetadata.pipeline_id == pipeline_execution.pipeline_id).update({
                "status": 'Completed',
                "pipeline_end_datetime": datetime.utcnow()
            })
            db.commit()

            db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == pipeline.job_execution_id).update({
                "status": "Completed",
                "end_datetime": datetime.utcnow()
            })
            db.commit()



        

    







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
        db_transformation = TransformationMetadata(called_by="Job", status="In Progress", transformation_detail=job.job_detail["Transformation"])
        db.add(db_transformation)
        db.commit()
        db.refresh(db_transformation)

        db_transJobPair = TransformationJobPair(transformation_id=db_transformation.transformation_id, job_execution_id=db_status.job_execution_id)
        db.add(db_transJobPair)
        db.commit()
        db.refresh(db_transJobPair)

    elif job_type == 'Ingestion':
        db_ingestion = IngestionMetadata(called_by="Job", status="In Progress", ingestion_detail=job.job_detail["Ingestion"])
        db.add(db_ingestion)
        db.commit()
        db.refresh(db_ingestion)

        db_ingestJobPair = IngestionJobPair(ingestion_id=db_ingestion.ingestion_id, job_execution_id=db_status.job_execution_id)
        db.add(db_ingestJobPair)
        db.commit()
        db.refresh(db_ingestJobPair)

    elif job_type == 'Pipeline':
        db_pipeline = PipelineMetadata(job_execution_id=db_status.job_execution_id, pipeline_detail=job.job_detail["Pipeline"], 
                                       total_task_count=len(job.job_detail["Pipeline"]), current_running_task_number=1,
                                       status="Running")
        db.add(db_pipeline)
        db.commit()
        db.refresh(db_pipeline)

        pipeline_list = db_pipeline.pipeline_detail
        first_task = pipeline_list[0]
        first_task_type = list(first_task.keys())[0]
        db_pipeline_execution = PipelineExecutionStatus(pipeline_id=db_pipeline.pipeline_id, task_number=1,
                                                        task_type=first_task_type, status="Running")
        db.add(db_pipeline_execution)
        db.commit()
        db.refresh(db_pipeline_execution)

        if first_task_type == 'Transformation':
            db_pipeline_transformation = TransformationMetadata(called_by="Pipeline", status="In Progress", transformation_detail=first_task["Transformation"])
            db.add(db_pipeline_transformation)
            db.commit()
            db.refresh(db_pipeline_transformation)

            db_transPipePair = TransformationPipelinePair(transformation_id=db_pipeline_transformation.transformation_id, pipeline_execution_id=db_pipeline_execution.pipeline_execution_id)
            db.add(db_transPipePair)
            db.commit()
            db.refresh(db_transPipePair)

        elif first_task_type == 'Ingestion':
            db_pipeline_ingestion = IngestionMetadata(called_by="Pipeline", status="In Progress", ingestion_detail=first_task["Ingestion"])
            db.add(db_pipeline_ingestion)
            db.commit()
            db.refresh(db_pipeline_ingestion)

            db_ingestPipePair = IngestionPipelinePair(ingestion_id=db_pipeline_ingestion.ingestion_id, pipeline_execution_id=db_pipeline_execution.pipeline_execution_id)
            db.add(db_ingestPipePair)
            db.commit()
            db.refresh(db_ingestPipePair)
    
    
    else:
        db_status = "Job Type not supported"

    return {"Message": "Job exicution started", "Execution Info": db_status}





@router.patch("/transformations/{transformation_id}/failed/", tags=["dummies"])
def fail_jobs(transformation_id: int, error_message: str, db : Session = Depends(get_db)):
    """
    For manually setting a transformation as failed with error.
    """
    transformation = db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_id).first()
    if transformation is None:
        db.close()
        raise HTTPException(status_code=404, detail="Transformation not found")
    called_by = transformation.called_by
    if transformation.status != "In Progress":
        db.close()
        raise HTTPException(status_code=403, detail="Transformation is not in progress")
    db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_id).update({
        "status": "Failed",
        "transformation_end_datetime": datetime.utcnow(),
        "error_message":error_message
    })
    db.commit()
    
    if called_by == 'Job':
        job_pair = db.query(TransformationJobPair).filter(TransformationJobPair.transformation_id == transformation_id).first()
        db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == job_pair.job_execution_id).update({
            "status": "Failed",
            "end_datetime": datetime.utcnow(),
            "error_message": "Transformation fails with message: < " + error_message + " >"
        })
        db.commit()
    elif called_by == 'Pipeline':
        pipeline_pair = db.query(TransformationPipelinePair).filter(TransformationPipelinePair.transformation_id == transformation_id).first()
        update_pipeline_execution_status(pipeline_execution_id=pipeline_pair.pipeline_execution_id, status="Failed", error_message=error_message, db=db)
    

    db.close()
    return {"message": "Job execution status and end datetime updated successfully"}







@router.patch("/transformations/{transformation_id}/completed/", tags=["dummies"])
def end_jobs(transformation_id: int, db : Session = Depends(get_db)):
    """
    For manually setting a transformation execution as complete.
    """
    transformation = db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_id).first()
    if transformation is None:
        db.close()
        raise HTTPException(status_code=404, detail="Transformation not found")
    called_by = transformation.called_by
    if transformation.status != "In Progress":
        db.close()
        raise HTTPException(status_code=403, detail="Transformation is not in progress")
    db.query(TransformationMetadata).filter(TransformationMetadata.transformation_id == transformation_id).update({
        "status": "Completed",
        "transformation_end_datetime": datetime.utcnow()
    })
    db.commit()
    
    if called_by == 'Job':
        job_pair = db.query(TransformationJobPair).filter(TransformationJobPair.transformation_id == transformation_id).first()
        db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == job_pair.job_execution_id).update({
            "status": "Completed",
            "end_datetime": datetime.utcnow()
        })
        db.commit()
    
    elif called_by == 'Pipeline':
        pipeline_pair = db.query(TransformationPipelinePair).filter(TransformationPipelinePair.transformation_id == transformation_id).first()
        update_pipeline_execution_status(pipeline_execution_id=pipeline_pair.pipeline_execution_id, status="Completed", db=db)
    
    

    db.close()
    return {"message": "Transformation execution status and end datetime updated successfully"}








@router.patch("/ingestions/{ingestions_id}/failed/", tags=["dummies"])
def fail_jobs(ingestion_id: int, error_message: str, db : Session = Depends(get_db)):
    """
    For manually setting a ingestion as failed with error.
    """
    ingestion = db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_id).first()
    if ingestion is None:
        db.close()
        raise HTTPException(status_code=404, detail="Ingestion not found")
    called_by = ingestion.called_by
    if ingestion.status != "In Progress":
        db.close()
        raise HTTPException(status_code=403, detail="Ingestion is not in progress")
    db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_id).update({
        "status": "Failed",
        "ingestion_end_datetime": datetime.utcnow(),
        "error_message":error_message
    })
    db.commit()
    
    if called_by == 'Job':
        job_pair = db.query(IngestionJobPair).filter(IngestionJobPair.ingestion_id == ingestion_id).first()
        db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == job_pair.job_execution_id).update({
            "status": "Failed",
            "end_datetime": datetime.utcnow(),
            "error_message": "Ingestion fails with message: < " + error_message + " >"
        })
        db.commit()
    elif called_by == 'Pipeline':
        pipeline_pair = db.query(IngestionPipelinePair).filter(IngestionPipelinePair.ingestion_id == ingestion_id).first()
        update_pipeline_execution_status(pipeline_execution_id=pipeline_pair.pipeline_execution_id, status="Failed", error_message=error_message, db=db)
    

    db.close()
    return {"message": "Job execution status and end datetime updated successfully"}







@router.patch("/ingestions/{ingestion_id}/completed/", tags=["dummies"])
def end_jobs(ingestion_id: int, db : Session = Depends(get_db)):
    """
    For manually setting a ingestion execution as complete.
    """
    ingestion = db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_id).first()
    if ingestion is None:
        db.close()
        raise HTTPException(status_code=404, detail="Ingestion not found")
    called_by = ingestion.called_by
    if ingestion.status != "In Progress":
        db.close()
        raise HTTPException(status_code=403, detail="Ingestion is not in progress")
    db.query(IngestionMetadata).filter(IngestionMetadata.ingestion_id == ingestion_id).update({
        "status": "Completed",
        "ingestion_end_datetime": datetime.utcnow()
    })
    db.commit()
    
    if called_by == 'Job':
        job_pair = db.query(IngestionJobPair).filter(IngestionJobPair.ingestion_id == ingestion_id).first()
        db.query(JobExecutionStatus).filter(JobExecutionStatus.job_execution_id == job_pair.job_execution_id).update({
            "status": "Completed",
            "end_datetime": datetime.utcnow()
        })
        db.commit()
    elif called_by == 'Pipeline':
        pipeline_pair = db.query(IngestionPipelinePair).filter(IngestionPipelinePair.ingestion_id == ingestion_id).first()
        update_pipeline_execution_status(pipeline_execution_id=pipeline_pair.pipeline_execution_id, status="Completed", db=db)
    
    

    db.close()
    return {"message": "Ingestion execution status and end datetime updated successfully"}









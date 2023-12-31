from fastapi import FastAPI
from data.database import engine
from data.model import Base
from apis import alpha, jobsAPI, pipelineAPI, transformationAPI, ingestionAPI, dummyAPI
from info.app_metadata import description, tags_metadata, contact



app = FastAPI(
    title="DoCC Alpha API",
    description=description,
    summary="Connecting to external databases",
    version="0.0.1",
    terms_of_service="http://example.com/terms/",
    contact=contact,
    openapi_tags=tags_metadata,
)


# Create the database tables if they don't already exist
Base.metadata.create_all(bind=engine, checkfirst=True)


@app.get("/")
def show_root():
    return {"Hello": "Please go to '<url>/docs' to view API end points in swagger form"}

# Different API paths
app.include_router(alpha.router)
# app.include_router(dahbaord_retrival_code.router)
# app.include_router(fetch_pipeline_info.router)
app.include_router(jobsAPI.router)
app.include_router(pipelineAPI.router)
app.include_router(transformationAPI.router)
app.include_router(ingestionAPI.router)
app.include_router(dummyAPI.router)
from fastapi import FastAPI
from data.database import engine
from data.model import Base
from apis import alpha, dahbaord_retrival_code, fetch_pipeline_info
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
    return {"Hello": "Go to 'url/docs' to view API end points"}

# Different API paths
app.include_router(alpha.router)
app.include_router(dahbaord_retrival_code.router)
app.include_router(fetch_pipeline_info.router)

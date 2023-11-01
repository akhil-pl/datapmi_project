from fastapi import APIRouter, Depends, Query, HTTPException
from enum import Enum
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session, sessionmaker
from pydantic import BaseModel

from data.database import get_db
from data.model import Connections
from functions.meta_func import metadata_dict, get_primary_keys, table_list

router = APIRouter()

# A table need to be created in database for storing supported database along with description and credential requirements
class SupportedDatabases(str, Enum):
    mysql = "mysql"
    postgres = "postgresql"

class DatabaseCredentials(BaseModel):
    source: SupportedDatabases
    host: str
    port: str
    user: str
    password: str
    database: str


# Path to add a new connection
@router.post("/connections/add", tags=["connection"])
async def create_new_connection(
    source: SupportedDatabases,
    host: str = Query(..., description="Host URL"),
    user: str = Query(..., description="User name"),
    password: str = Query(..., description="User password"),
    port: str = Query(..., description="Port number"),
    database: str = Query(..., description="Database name"),
    db: Session = Depends(get_db)
):  
    """Path to add a new connection"""
    connection_data = {
        "source": source,
        "host": host,
        "user": user,
        "password": password,
        "port": port,
        "database": database
    }
    # Need to Validate the connection before saving to the database
    # Need to Encrypt the password before storing in the database
    connection = Connections(**connection_data)
    db.add(connection)
    db.commit()
    db.refresh(connection)
    return connection










# API to Get Connection Details
@router.get("/connections/{connection_id}", tags=["connection"])
def get_connection_details(connection_id: int, db: Session = Depends(get_db)):
    """
    Get connection details by providing a connection ID.
    """
    # Fetch and return connection details based on the connection_id.
    connection = db.query(Connections).filter(Connections.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    return {"connection_id": connection_id, "details": connection}










# API to Perform a Connection to Pull Metadata
@router.post("/connections/connect", tags=["connection"])
def perform_connection_to_source(source_info: DatabaseCredentials):
    """
    Perform a connection to a data source and pull metadata.
    """
    # Use source_info to connect to the source and pull metadata.
    source_db_url = ""
    db = source_info.source
    connection_string = source_info.user+':'+source_info.password+'@'+source_info.host+':'+source_info.port+'/'+source_info.database
    try:
        if db not in SupportedDatabases:
            return {"error": "Unsupported database type"}
        if db in ["mysql", "postgresql"]:
            if db == 'mysql':
                source_db_url = "mysql+mysqlconnector://"+connection_string
            else:
                source_db_url = "postgresql://"+connection_string
            
            SessionLocal = sessionmaker()
            engine = create_engine(source_db_url)
            SessionLocal.configure(bind=engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            return {"source_info": source_info, "metadata": metadata_dict(metadata.tables.values())}
    except Exception as e:
        return {"error": str(e)}
    









# API to Provide a List of Different Sources
@router.get("/sources", tags=["connection"])
def list_supported_sources():
    """
    Provide a list of supported data sources.
    """
    # Return a list of supported data sources.
    # Need to be fetched from database once the table is created
    return {"supported_sources": list(SupportedDatabases)}









# API to Provide a List of Unique Identifiers for a Table in a Source
@router.get("/sources/{source}/tables/{table}/unique-identifiers", tags=["connection"])
def get_unique_identifiers(source: str, table: str, db: Session = Depends(get_db)):
    """
    Get the unique identifiers for a table in a source.
    """
    # Implement logic to retrieve unique identifiers for the specified table.
    connection = db.query(Connections).filter(Connections.id == source).first()
    source_db_url = ""
    dbs = connection.source
    if connection.password == "None":
        password = ""
    else:
        password = connection.password
    connection_string = connection.user+':'+password+'@'+connection.host+':'+connection.port+'/'+connection.database
    try:
        if dbs not in ["mysql", "postgresql"]:
            return {"error": "Unsupported database type"}
        if dbs in ["mysql", "postgresql"]:
            if dbs == 'mysql':
                source_db_url = "mysql+mysqlconnector://"+connection_string
            else:
                source_db_url = "postgresql://"+connection_string
            
            SessionLocal = sessionmaker()
            engine = create_engine(source_db_url)
            SessionLocal.configure(bind=engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            unique_identifiers = get_primary_keys(metadata, table)
            if unique_identifiers:
                return {"unique-identifiers": unique_identifiers}
            else:
                return {"error": f"Table {table} not found in database."}
    except Exception as e:
        return {"error": str(e)}
    









# API to Fetch Metadata from a Source Connection's Tables
@router.get("/connections/{connection_id}/tables/metadata", tags=["connection"])
def get_source_connection_tables_metadata(connection_id: int, db: Session = Depends(get_db)):
    """
    Fetch metadata for tables in a source connection.
    """
    # Implement logic to retrieve table metadata for the specified connection.
    connection = db.query(Connections).filter(Connections.id == connection_id).first()
    source_db_url = ""
    dbs = connection.source
    if connection.password == "None":
        password = ""
    else:
        password = connection.password
    connection_string = connection.user+':'+password+'@'+connection.host+':'+connection.port+'/'+connection.database
    try:
        if dbs not in ["mysql", "postgresql"]:
            return {"error": "Unsupported database type"}
        if dbs in ["mysql", "postgresql"]:
            if dbs == 'mysql':
                source_db_url = "mysql+mysqlconnector://"+connection_string
            else:
                source_db_url = "postgresql://"+connection_string
            
            SessionLocal = sessionmaker()
            engine = create_engine(source_db_url)
            SessionLocal.configure(bind=engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            return {"metadata": metadata_dict(metadata.tables.values())}
    except Exception as e:
        return {"error": str(e)}
    









# API to Provide a List of Data Source Tables in a Connection
@router.get("/connections/{connection_id}/tables", tags=["connection"])
def list_source_connection_tables(connection_id: int, db: Session = Depends(get_db)):
    """
    Provide a list of tables available in a source connection.
    """
    # Implement logic to list tables in the specified connection.
    connection = db.query(Connections).filter(Connections.id == connection_id).first()
    source_db_url = ""
    dbs = connection.source
    if connection.password == "None":
        password = ""
    else:
        password = connection.password
    connection_string = connection.user+':'+password+'@'+connection.host+':'+connection.port+'/'+connection.database
    try:
        if dbs not in ["mysql", "postgresql"]:
            return {"error": "Unsupported database type"}
        if dbs in ["mysql", "postgresql"]:
            if dbs == 'mysql':
                source_db_url = "mysql+mysqlconnector://"+connection_string
            else:
                source_db_url = "postgresql://"+connection_string
            
            SessionLocal = sessionmaker()
            engine = create_engine(source_db_url)
            SessionLocal.configure(bind=engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            return {"metadata": table_list(metadata.tables.values())}
    except Exception as e:
        return {"error": str(e)}
    









# API to Provide List of Connection Sources
@router.get("/connections/sources/all", tags=["connection"]) # Modified url path as it gives error with a previous path
def list_connection_sources(db: Session = Depends(get_db)):
    """
    Provide a list of available connection sources.
    """
    # Return a list of available connection sources.
    connections = db.query(Connections).all()
    return {"available connections": connections}









# API to Trigger a Join from Two Source Table Connections
@router.post("/joins", tags=["connection"])
def perform_join(joint_info: dict):
    """
    Perform a join operation between two source table connections.
    """
    # Implement logic to perform the join operation and return the result.
    return {...}
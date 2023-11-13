from fastapi import APIRouter, Depends, Query, HTTPException
from enum import Enum
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session, sessionmaker
from pydantic import BaseModel
from typing import Optional
import subprocess

from data.database import get_db
from data.model import Connections
from functions.metadata_manage_func import metadata_dict, get_primary_keys, table_list
from functions.dbt_yml_file_func import add_new_profiles_yml, update_target_profiles_yaml
from functions.dbt_sql_file_func import create_sql_query, delete_sql_file

import yaml

router = APIRouter()

# A table need to be created in database for storing supported database along with description and credential requirements
class SupportedDatabases(str, Enum):
    mysql = "mysql"
    postgres = "postgres"

class DatabaseCredentials(BaseModel):
    source: SupportedDatabases
    host: str
    port: str
    user: str
    password: str
    database: str

class SupportedJoins(str, Enum):
    cross = "CROSS"
    inner = "INNER"
    left = "LEFT"
    right = "RIGHT"
    full = "FULL"
    self = "SELF"

class ViewOrTable(str, Enum):
    table = "table"
    view = "view"

class JoinDetails(BaseModel):
    connection_id: int
    make : ViewOrTable
    type: SupportedJoins
    new_table : str
    table1: Optional[str] = None
    table1_col: Optional[dict] = None
    table2: Optional[str] = None
    table2_col: Optional[dict] = None


# Path to add a new connection
@router.post("/connections/add", tags=["connection"])
async def create_new_connection(
    source: SupportedDatabases,
    host: str = Query(..., description="Host URL"),
    user: str = Query(..., description="User name"),
    password: str = Query(None, description="User password"),
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
    source_db_url = ""
    dataB = source
    if password:
        pswd = password
    else:
        pswd = ""
    connection_string = user + ':' + pswd + '@' + host + ':' + port + '/' + database
    try:
        if dataB not in SupportedDatabases:
            return {"error": "Unsupported database type"}
        if dataB in ["mysql", "postgres"]:
            if dataB == 'mysql':
                source_db_url = "mysql+mysqlconnector://"+connection_string
            else:
                source_db_url = "postgresql://"+connection_string
            
            SessionLocal = sessionmaker()
            engine = create_engine(source_db_url)
            with engine.connect() as connection:
                message = "Connection succesfull"
    except Exception as e:
        return {"Message": "Connection error, check", "error": str(e)}
    else:
        # Adding connection details to database
        # Need to Encrypt the password before storing in the database
        connection = Connections(**connection_data)
        db.add(connection)
        db.commit()
        db.refresh(connection)

        # Update profile.yml file with connection details
        add_new_profiles_yml(connection=connection)
        
        return {"Message": message, "Connection": connection}










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
        if db in ["mysql", "postgres"]:
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
    try:
        connection = db.query(Connections).filter(Connections.id == source).first()
        if not connection:
            return HTTPException(status_code=404, detail="Connection not found")
        
        source_db_url = ""
        dbs = connection.source
        if connection.password == "None":
            password = ""
        else:
            password = connection.password
        connection_string = connection.user+':'+password+'@'+connection.host+':'+connection.port+'/'+connection.database
    
        if dbs not in ["mysql", "postgres"]:
            return {"error": "Unsupported database type"}
        if dbs in ["mysql", "postgres"]:
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
    try:
        connection = db.query(Connections).filter(Connections.id == connection_id).first()
        if not connection:
            return HTTPException(status_code=404, detail="Connection not found")
        
        source_db_url = ""
        dbs = connection.source
        if connection.password == "None":
            password = ""
        else:
            password = connection.password
        
        connection_string = connection.user+':'+password+'@'+connection.host+':'+connection.port+'/'+connection.database
    
        if dbs not in ["mysql", "postgres"]:
            return {"error": "Unsupported database type"}
        if dbs in ["mysql", "postgres"]:
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
    try:
        connection = db.query(Connections).filter(Connections.id == connection_id).first()
        if not connection:
            return HTTPException(status_code=404, detail="Connection not found")
        
        source_db_url = ""
        dbs = connection.source
        if connection.password == "None":
            password = ""
        else:
            password = connection.password
        
        connection_string = connection.user+':'+password+'@'+connection.host+':'+connection.port+'/'+connection.database
        
        if dbs not in ["mysql", "postgres"]:
            return {"error": "Unsupported database type"}
        if dbs in ["mysql", "postgres"]:
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
def perform_join(joint_info: JoinDetails, db: Session = Depends(get_db)):
    """
    Perform a join operation between two source table connections.
    Only CROSS table implemented now. Required columns of final table to be given as {"source name":"destination name"} pair from both table
    """
    # Implement logic to perform the join operation and return the result.
    connection_id = joint_info.connection_id
    join_make = joint_info.make
    join_type = joint_info.type
    new_table = joint_info.new_table
    table1 = joint_info.table1
    table1_col = joint_info.table1_col
    table2 = joint_info.table2
    table2_col = joint_info.table2_col

    connection = db.query(Connections).filter(Connections.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    # Update target value on profiles.yml file
    update_target_profiles_yaml(connection.id)

    # Create .sql file with query
    sql_query = create_sql_query(join_make=join_make, join_type=join_type, new_table=new_table, table1=table1, table1_col=table1_col, table2=table2, table2_col=table2_col)
    
    # Use subprocess to run dbt
    project_location = "./dbt/postgres_dbt"
    result = subprocess.run(["dbt", "run", "--project-dir", project_location], capture_output=True, text=True)

    # Delete the .sql file after suscessful running of dbt project
    delete_sql_file(new_table=new_table)

    # dbt does not return tables, so need to get new table using SQLAlchemy and return

    return {"SQL Query": sql_query, "stdout": result.stdout, "stderr": result.stderr}
    
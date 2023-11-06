from fastapi import APIRouter, Depends, Query, HTTPException
from enum import Enum
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import Session, sessionmaker
from pydantic import BaseModel
from typing import Optional
import subprocess

from data.database import get_db
from data.model import Connections
from functions.meta_func import metadata_dict, get_primary_keys, table_list

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
    inner = "INNER"
    left = "LEFT"
    right = "RIGHT"
    full = "FULL"
    self = "SELF"
    cross = "CROSS"

class JoinDetails(BaseModel):
    connection_id: int
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
    # Need to Encrypt the password before storing in the database

    
    # Adding connection details to database
    connection = Connections(**connection_data)
    db.add(connection)
    db.commit()
    db.refresh(connection)

    # Update profile.yml file with connection details
    with open('/home/user/.dbt/profiles.yml', 'r') as file: # Read the YAML file
        data = yaml.load(file, Loader=yaml.FullLoader)
    
    test_output = { # Need to give unique keys, or better to provide while execution only
        "id_"+str(connection.id): {
            'type': connection.source,
            'threads': 1,
            'host': connection.host,
            'port': int(connection.port),
            'user': connection.user,
            'pass': connection.password,
            'dbname': connection.database,
            'schema': 'public',
        }
    }
    
    data['postgres_dbt']['outputs'].update(test_output)
    
    with open('/home/user/.dbt/profiles.yml', 'w') as file: # Write the updated data structure back to the YAML file
        yaml.dump(data, file, default_flow_style=False)

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
def perform_join(joint_info: JoinDetails, db: Session = Depends(get_db)):
    """
    Perform a join operation between two source table connections.
    """
    # Implement logic to perform the join operation and return the result.
    connection_id = joint_info.connection_id
    join_type = joint_info.type
    new_table = joint_info.new_table
    table1 = joint_info.table1
    table1_col = joint_info.table1_col
    table2 = joint_info.table2
    table2_col = joint_info.table2_col

    connection = db.query(Connections).filter(Connections.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    with open('/home/user/.dbt/profiles.yml', 'r') as file: # Read the YAML file
        data = yaml.load(file, Loader=yaml.FullLoader)
    
    data['postgres_dbt']['target'] = "id_"+str(connection.id)
    
    with open('/home/user/.dbt/profiles.yml', 'w') as file: # Write the updated data structure back to the YAML file
        yaml.dump(data, file, default_flow_style=False)

    
    # Create appropriate .sql file
    # Specify the directory and filename for the .sql file
    directory = "./dbt/postgres_dbt/models"
    filename = new_table + ".sql"
    sql_file_path = f"{directory}/{filename}"


    # Build the SQL query dynamically
    sql_query = f"SELECT\n\t"
    # Generate SELECT clause for table1
    select_table1 = [f"{table1}.{col} AS {table1_col[col]}" for col in table1_col]
    sql_query += ",\n\t".join(select_table1)
    sql_query += f",\n\t"

    # Generate SELECT clause for table2
    select_table2 = [f"{table2}.{col} AS {table2_col[col]}" for col in table2_col]
    sql_query += ",\n\t".join(select_table2)

    # Add the FROM clause with the join type and tables
    sql_query += f"\nFROM {table1}\n{join_type} JOIN {table2}"


    # Open the .sql file for writing
    with open(sql_file_path, "w") as sql_file:
        # Write SQL queries to the file, one query per line
        sql_file.write(sql_query)

    # The .sql file is now created with your SQL queries


    project_location = "./dbt/postgres_dbt"

    # Use subprocess or another method to run dbt commands from the remote location
    result = subprocess.run(["dbt", "run", "--project-dir", project_location], capture_output=True, text=True)
    return {"stdout": result.stdout, "stderr": result.stderr}
    
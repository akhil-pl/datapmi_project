from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

# Create an MySQL database engine
connection_string = "mysql+mysqlconnector://user:@localhost:3306/datapmi_project"
engine = create_engine(connection_string, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Function to be used in ascess db
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Get database metadata
metadata = MetaData()
metadata.reflect(bind=engine)

def get_db_metadata():
    return metadata.tables.values()

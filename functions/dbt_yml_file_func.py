# Need to add functions for manipulating .yml files here

import os
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Function to read profiles.yml file
def read_profiels_yml():
    try:
        # Get the file path from the environment variable
        file_path = os.getenv('DBT_PROFILES_PATH')

        if not file_path:
            raise ValueError("DBT_PROFILES_PATH is not set in the .env file")

        with open(file_path, 'r') as file: # Read the YAML file
            data = yaml.load(file, Loader=yaml.FullLoader)
    except Exception as e:
        # Handle exceptions, (log the error or raise a custom exception)
        return {"success": False, "error": str(e)}
    else:
        return data



# Function to write to profiles.yml file
def write_profiles_yml(data):
    try:
        # Get the file path from the environment variable
        file_path = os.getenv('DBT_PROFILES_PATH')

        if not file_path:
            raise ValueError("DBT_PROFILES_PATH is not set in the .env file")

        with open(file_path, 'w') as file:
            yaml.dump(data, file, default_flow_style=False)
    except Exception as e:
        # Handle exceptions, (log the error or raise a custom exception)
        return {"success": False, "error": str(e)}
    else:
        return {"success": True}



# Function to add connection details to profiles.yml file
def add_new_profiles_yml(connection):
    data = read_profiels_yml()

    test_output = { # Need to give unique keys, or better to provide connection id
        "id_"+str(connection.connection_id): {
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

    result = write_profiles_yml(data=data)
    return result



# Function for updating target value of profiles.yml file
def update_target_profiles_yaml(connection_id):
    data = read_profiels_yml()

    data['postgres_dbt']['target'] = "id_"+str(connection_id)

    result = write_profiles_yml(data=data)
    return result
# Need to add functions for manipulating .yml files here

import yaml


# Function to read profiles.yml file
def read_profiels_yml():
    try:
        with open('/home/user/.dbt/profiles.yml', 'r') as file: # Read the YAML file
            data = yaml.load(file, Loader=yaml.FullLoader)
    except Exception as e:
        # Handle exceptions, (log the error or raise a custom exception)
        return {"success": False, "error": str(e)}
    else:
        return data



# Function to write to profiles.yml file
def write_profiles_yml(data):
    try:
        with open('/home/user/.dbt/profiles.yml', 'w') as file:
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

    result = write_profiles_yml(data=data)
    return result



# Function for updating target value of profiles.yml file
def update_target_profiles_yaml(id):
    data = read_profiels_yml()

    data['postgres_dbt']['target'] = "id_"+str(id)

    result = write_profiles_yml(data=data)
    return result
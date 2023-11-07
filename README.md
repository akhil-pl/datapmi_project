# datapmi_project

## Run project 

- Start a virtual environment and acticvate
- pip install packages in the requirements.txt file
- Change the database credentials (connection_string) inside "./data/database.py" file
- Should have crated a dbt folder in the system, so that a 'profiles.yml' file is present in this path '/home/user/.dbt/'
- Add this content inside "profiles.yml" file
```profiles.yml
postgres_dbt:
  outputs:
    prod:
      dbname:
      - dbname
      host:
      - host
      pass:
      - prod_password
      port:
      - port
      schema:
      - prod_schema
      threads:
      - 1 or more
      type: postgres
      user:
      - prod_username
  target: prod
```
- Now run the fastapi app
# Need to add functions for manipulating .sql files here

import os


# Function to create and write to .sql file
def create_sql_file(new_table, sql_query):
    try:
        # Create appropriate .sql file
        # Specify the directory and filename for the .sql file
        directory = "./dbt/postgres_dbt/models"
        filename = new_table + ".sql"
        sql_file_path = f"{directory}/{filename}"

        # Open the .sql file for writing
        with open(sql_file_path, "w") as sql_file:
            # Write SQL queries to the file, one query per line
            sql_file.write(sql_query)
    except Exception as e:
        # Handle exceptions, (log the error or raise a custom exception)
        return {"success": False, "error": str(e)}
    else:
        return {"success": True}



# Function to create CROSS join query
def cross_join_query(sql_query, table1:str, table1_col, table2:str, table2_col):
    sql_query += f"SELECT\n\t"
    # Generate SELECT clause for table1
    select_table1 = [f"{table1}.{col} AS {table1_col[col]}" for col in table1_col]
    sql_query += ",\n\t".join(select_table1)
    sql_query += f",\n\t"

    # Generate SELECT clause for table2
    select_table2 = [f"{table2}.{col} AS {table2_col[col]}" for col in table2_col]
    sql_query += ",\n\t".join(select_table2)

     # Add the FROM clause with the join type and tables
    sql_query += f"\nFROM {table1}\nCROSS JOIN {table2}"

    return sql_query


# Function to create CROSS join query
def match_join_query(sql_query, join_type, table1:str, table1_col, table2:str, table2_col, match_pair):
    sql_query += f"SELECT\n\t"
    # Generate SELECT clause for table1
    select_table1 = [f"{table1}.{col} AS {table1_col[col]}" for col in table1_col]
    sql_query += ",\n\t".join(select_table1)
    sql_query += f",\n\t"

    # Generate SELECT clause for table2
    select_table2 = [f"{table2}.{col} AS {table2_col[col]}" for col in table2_col]
    sql_query += ",\n\t".join(select_table2)

     # Add the FROM clause with the join type and tables
    sql_query += f"\nFROM {table1}\n{join_type} JOIN {table2}"

    # Add match columns
    sql_query += f"\nON\n\t"
    match_columns = [f"{table1}.{col} = {table2}.{match_pair[col]}" for col in match_pair]
    sql_query += "AND\n\t".join(match_columns)

    return sql_query




# Function to create sql query dynamically
def create_sql_query(join_make, join_type, new_table, table1, table1_col, table2, table2_col, match_pair):
    # Build the SQL query dynamically
    sql_query = ""
    if join_make == "table":
        sql_query = "{{ config(materialized='table') }}"
        sql_query += f"\n\n"
    
    if join_type == "CROSS":
        sql_query = cross_join_query(sql_query=sql_query, table1=table1, table1_col=table1_col, table2=table2, table2_col=table2_col)
    elif join_type in ["INNER", "LEFT", "RIGHT", "FULL"]:
        sql_query = match_join_query(sql_query=sql_query, join_type=join_type, table1=table1, table1_col=table1_col, table2=table2, table2_col=table2_col,match_pair=match_pair)
    elif join_type == "SELF":
        return {"success": False, "error": "SELF join not implemented yet"}
    else:
        return {"success": False, "error": "Unsupported join type"}

    create_sql_file(new_table=new_table, sql_query=sql_query)

    return sql_query


# Function to delete sql file
def delete_sql_file(new_table):
    directory = "./dbt/postgres_dbt/models"
    filename = new_table + ".sql"
    sql_file_path = f"{directory}/{filename}"
    os.remove(sql_file_path)

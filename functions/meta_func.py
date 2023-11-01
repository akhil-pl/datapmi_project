# Functions for creating required dict from metadata


# Function to create dictionary object from metadata
def metadata_dict(data):
    Table = {}
    for table in data:
        table_info = {}  # Create a dictionary for table-specific metadata

        # Add table name
        table_info['name'] = table.name
        
        # Get primary keys for the table
        primary_keys = [key.name for key in table.primary_key]
        table_info['primary_keys'] = primary_keys

        # Get constraints for the table
        constraints = []
        for constraint in table.constraints:
            constraints.append({
                'name': constraint.name,
                'type': str(constraint),
            })
        table_info['constraints'] = constraints

        # Get indexes for the table
        indexes = []
        for index in table.indexes:
            indexes.append({
                'name': index.name,
                'columns': [col.name for col in index.columns]
            })
        table_info['indexes'] = indexes

        table_info['columns'] = {}  # Create a dictionary for column information
        # Populate the table-specific metadata
        for column in table.c:
            column_info = {
                'type': str(column.type),
                'nullable': column.nullable,
                'default': column.default,
            }
            # Add column information to the table-specific metadata
            table_info['columns'][column.name] = column_info
        
        # Get foreign keys for the table
        foreign_keys = []
        for column in table.columns:
            if column.foreign_keys:
                for fk in column.foreign_keys:
                    foreign_keys.append({
                        'column': column.name,
                        'foreign_table': fk.column.table.name,
                        'foreign_column': fk.column.name
                    })
        table_info['foreign_keys'] = foreign_keys

        # Table Comment
        if table.comment is not None:
            table_info['table_comment'] = table.comment
            
        # Add table metadata to the 'Table' dictionary
        Table[table.name] = table_info
    return Table


# This Module contains SQL generation for Testing Framework checks
#   Function takes table names and rules as parameter and returns a generated SQL
#   

def null_check(object_name,column_name):
    try:
        Full_table_name=object_name.strip()
        parts = Full_table_name.split('.')

        database, schema, table_name = [p.upper() for p in parts]

        condition=" OR ".join([f"{col} IS NULL" for col in column_name])


        generated_sql=f"""
                    SELECT count(*) as result
                    FROM {database}.{schema}.{table_name}
                    WHERE {condition};
                    """
        return generated_sql.strip()
    except Exception as e:
        raise ValueError(f"Error building null_check: {str(e)}")


def table_exists(object_name):   
    """Check if a table exists in Snowflake"""
    try:
        Full_table_name=object_name.strip()
        parts = Full_table_name.split('.')

        database, schema, table_name = [p.upper() for p in parts]

        generated_sql=f"""
                    SELECT count(*) as result
                    FROM {database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA='{schema}'
                    AND TABLE_NAME='{table_name}';
                    """
        return generated_sql.strip()
    except Exception as e:
        raise ValueError(f"Error building table_exists check: {str(e)}")

def duplicate_check():
    pass


def stage_exists(object_name):
    pass



sanity_registry= {
    "stage_exists" : stage_exists,
    "table_exists" : table_exists,
    "null_check" : null_check,
    "duplicate_check" : duplicate_check

}

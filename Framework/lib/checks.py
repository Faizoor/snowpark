# This Module contains SQL generation for Testing Framework checks
#   Function takes table names and rules as parameter and returns a generated SQL
#   

from snowflake.snowpark.functions import col


def null_check(object_name,column_names,session):
    try:
        Full_table_name=object_name.strip()
        parts = Full_table_name.split('.')

        database, schema, table_name = [p.upper() for p in parts]

        df = session.table(f"{database}.{schema}.{table_name}")

        # ensure requested columns actually exist in the table;
        existing_cols = [x.lower() for x in df.columns]
        missing = [c for c in column_names if c.lower() not in existing_cols]
        if missing:
            raise ValueError(f"Column(s) {missing} not found in {object_name}")

        condition = None

        for c in column_names:
            expr = col(c).is_null()
            condition = expr if condition is None else condition | expr

        df = df.filter(condition)

        return df

    except Exception as e:
        #print(str(e))
        raise ValueError(str(e))


def table_exists(object_name,session):
    """check if a table exists using snowpark dataframe"""
    try:
        Full_table_name=object_name.strip()
        parts = Full_table_name.split('.')

        database, schema, table_name = [p.upper() for p in parts]

        df=session.table(f"{database}.INFORMATION_SCHEMA.TABLES")\
                .filter(
                    (col("TABLE_SCHEMA")==schema) & 
                    (col("TABLE_NAME")==table_name)
                    )
        return df
        
    except Exception as e:
        raise ValueError(str(e)) 
  

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


def null_check_with_sql(object_name,column_name):
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
    
def table_exists_with_sql(object_name):   
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
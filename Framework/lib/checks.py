# This Module contains SQL generation for Testing Framework
#   Function takes table names and rules as parameter and returns a generated SQL
#   



def null_check(table_name,rule):
    pass


def unique_check():
    pass


def stage_exists(object_name):
    return f"{object_name}"

def table_exists(object_name):   
    Full_table_name=object_name.strip()
    database=Full_table_name.split('.')[0].upper()
    schema=Full_table_name.split('.')[1].upper()
    table_name=Full_table_name.split('.')[2].upper()
    generated_sql=f"""
                SELECT count(*) as result
                FROM {database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA='{schema}'
                AND TABLE_NAME='{table_name}';
                """
    return generated_sql.strip()
sanity_registry= {
    "stage_exists" : stage_exists,
    "table_exists" : table_exists

}

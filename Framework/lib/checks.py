# This Module contains SQL generation for Testing Framework
#   Function takes table names and rules as parameter and returns a generated SQL
#   



def null_check(table_name,rule):
    pass


def unique_check():
    pass


def stage_exists(database,schema,stage):
    return f"{database,schema, stage}"

def table_exists(database,schema,stage):
    return f"{database,schema, stage}"

sanity_registry= {
    "stage_exists" : stage_exists,
    "table_exists" : table_exists

}

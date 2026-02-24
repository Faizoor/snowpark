################################################################################################
# Snowpark Data Quality Testing Framework
################################################################################################
import yaml 
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os 
import logging 
from lib import checks


class SnowparkTesting():
    """ 
            Description about the class
    """

    def __init__(self,session,yaml_path):
        self.session= session
        self.yaml_path = yaml_path
        self.config = self.config_loader()
        #print(self.config.get('tables'))
        self.application = self.config.get("application")
        self.database = self.config.get("database")
        self.schema = self.config.get("schema")
        
    
        #logging the database, schema and applications details
        print(f"    Applicatoin: {self.application}")
        print(f"    Database:    {self.database}")
        print(f"    Schema:      {self.schema}")


    def execute(self):
        """ 
            Main function that will control the orchestration 
        """

        #Run sanity checks
        #for checks in self.config.get("checks")['sanity_checks']:
        #    print(checks)
        self.sanity_checks()

        #Data Quality checks
        tables = self.config.get("rules")

        for tables in tables:
            table_name=tables.get("name")
            enabled=tables.get("enabled",True)
            if not enabled:
                print(f"DQ check is not enabled for this table: , {self._build_table_name(table_name)}")
                continue

            qualified_table_name = self._build_table_name(table_name)
            print(f"running checks for the table")


    def sanity_checks(self):

        """
        Execute sanity checks before DQ validations
        
        """
        sanity_conf=self.config.get("sanity_checks")

        if not sanity_conf:
            return f"No sanity checks to perform"
        print(f"Running sanity checks")
        print(sanity_conf)
        for rule in sanity_conf.get('rules'):
            rule_id = rule.get('rule_id')
            name = rule.get('name') 
            rule_type=rule.get('rule_type')
            object_name=rule.get('object')
            object_type = rule.get('object_type')

            print(rule_id)
            print(name)
            print(object_name)
            print(rule_type)

            print(object_type)

            if rule_type not in checks.sanity_registry:
                raise ValueError(f"Unsupported sanity check")
            #print(checks.sanity_registry['stage_exists'])
            check_function = checks.sanity_registry[check_name]

            genereated_sql = check_function(self.database,self.schema,stages)

            print(genereated_sql)


                
    def _build_table_name(self,table_name):
        return f"{self.database.lower}.{self.schema.lower}.{table_name.lower()}"
    
    def config_loader(self):
        """
            Read the yaml file and returns it
        """
        with open(self.yaml_path,'r') as conf:
            return yaml.safe_load(conf)







if __name__ == "__main__":
    load_dotenv()
    yaml_path = "Framework/config.yaml"

    connection_parameters = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"], 
}

    #create session
    try:
        session = Session.builder.configs(connection_parameters).create()
    
    #create Framework Object 
        snowpark_testing = SnowparkTesting( session, yaml_path  )
        results=snowpark_testing.execute()
        #print(f"results: {results}")

    except Exception as e :
        print(f"Script aborted due to this error {type(e)}:  {e}")

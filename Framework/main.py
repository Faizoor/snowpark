################################################################################################
# Snowpark Data Quality Testing Framework
################################################################################################
import yaml 
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os 
import logging 
from lib import checks
import pandas as pd
import csv


class SnowparkValidationRunner():
    """ 
        Main class for running Sanity and Functional checks for Data Quality Testing Framework.
            - sanity_checks: checks to validate the existence of objects and basic checks before running DQ validations
            - functional_checks: checks to validate the data quality based on the rules defined in the config file
            parameters:
                session: Snowflake session object
                yaml_path: path to the yaml config file 
    """

    def __init__(self,session,yaml_path):
        """
            Initialize the class with session and yaml path, load the config.
        """

        self.session= session
        self.yaml_path = yaml_path
        self.config = self.config_loader()
        #logger.info(self.config.get('tables'))
        self.application = self.config.get("application")
        self.database = self.config.get("database")
        self.schema = self.config.get("schema")
        
    
        #logging the database, schema and applications details
        logger.info(f"    Applicatoin: {self.application}")
        logger.info(f"    Database:    {self.database}")
        logger.info(f"    Schema:      {self.schema}")


    def execute(self):
        """ 
            Main function that will control the orchestration 
        """

        #Run sanity checks
        sanity_check_result=self.sanity_checks()    
        #logger.info(f"Sanity check results: {sanity_check_result}")

        #Data Quality checks
        functional_check_result=self.functional_checks()

        #checks summary
        sanity_summary=self._get_summary(sanity_check_result)
        logger.info(f"Sanity checks summary: {sanity_summary}")
        functional_summary=self._get_summary(functional_check_result)
        logger.info(f"Functional checks summary: {functional_summary}")
    
        # return results
        return {
            "sanity":sanity_check_result,
            "Functional":functional_check_result,
           

        }


    def sanity_checks(self):

        """
        Execute sanity checks 
        
        """
        sanity_conf=self.config.get("sanity_checks")

        sanity_results=[]

        if not sanity_conf:
            return f"No sanity checks to perform"
        logger.info(f"Running sanity checks")
        #logger.info(sanity_conf)
        for rule in sanity_conf.get('rules'):
            rule_id = rule.get('rule_id')
            name = rule.get('name') 
            rule_type=rule.get('rule_type')
            object_name=rule.get('object')
            object_type = rule.get('object_type')

            error_message=None
            status=None

            #logger.info("rule_id: %s", rule_id)
            #logger.info("name: %s", name)
            #logger.info("object_name: %s", object_name)
            #logger.info("rule_type: %s", rule_type)
            #logger.info(object_type)

            logger.info(f"Executing sanity check: rule_id - {rule_id} - on object: {object_name} with rule type: {rule_type}")  

            if rule_type not in checks.sanity_registry:
                raise ValueError(f"Unsupported sanity check")
            #logger.info(checks.sanity_registry['stage_exists'])
            check_function = checks.sanity_registry[rule_type]
            try:
                sql = check_function(object_name)
                #logger.info(sql)

                result = self.session.sql(sql).collect()
                #logger.info(result)
                
                value = result[0]['RESULT']
                status="PASS" if value>0 else "FAIL"
                if status=="PASS":
                    logger.info(f"Sanity check: rule_id - {rule_id} - on object: {object_name} with rule type: {rule_type} has PASSED")
                else:
                    logger.info(f"Sanity check: rule_id - {rule_id} - on object: {object_name} with rule type: {rule_type} has FAILED")
            except Exception as e:  
                status = "ERROR"
                error_message=getattr(e, 'message', str(e))
                logger.error(f"Error executing sanity check with {rule_id} on object - {object_name}: {error_message}")

            sanity_results.append(
                    {
                        "rule_id":rule_id,
                        "name"   :name,
                        "object" :object_name,
                        "rule_type":rule_type,
                        "status" : status,
                        "error_message":error_message
                    }
                )
        return sanity_results

    def functional_checks(self):
        """
        Execute functional checks for DQ validations
        """

        functional_conf=self.config.get("functional_checks")

        functional_results=[]

        error_message=None
        status=None

        if not functional_conf:
            return f"No functional checks to perform"
        
        logger.info(f"Running functional checks")

        for rule in functional_conf.get('rules'):
            rule_id = rule.get('rule_id')
            name=rule.get('name')
            rule_type=rule.get('rule_type')
            object=rule.get('object')
            threshold=rule.get('threshold')

            #logger.info(f"rule_id: {rule_id}")
            #logger.info(f"name: {name}")
            #logger.info(f"rule_type: {rule_type}")
            #logger.info(f"object: {object}")

            print("Hi")
            if rule_type not in checks.sanity_registry:
                raise ValueError(f"Unsupported sanity check")

            check_function = checks.sanity_registry[rule_type]
            try:
                for obj in object:
                    object_name=obj.get('name')
                    columns=obj.get('columns')
                    #print(obj['name'])
                    #logger.info(f"object_name: {object_name}")
                    #logger.info(f"columns: {columns}")

                    #print(check_function.__name__)
                    #print(checks.null_check(object_name,columns))
                    sql = check_function(object_name,columns)
                    #logger.info(sql)
                    result = self.session.sql(sql).collect()
                    value = result[0]['RESULT']

                    status = "PASS" if value==threshold else "FAIL"
                    if status=="PASS":
                        logger.info(f"Functional check: rule_id - {rule_id} - on object: {object_name} with rule type: {rule_type} has PASSED")
                    else:
                        logger.info(f"Functional check: rule_id - {rule_id} - on object: {object_name} with rule type: {rule_type} has FAILED")

                    functional_results.append(
                        {
                            "rule_id": rule_id,
                            "name": name,
                            "object": object_name,
                            "rule_type": rule_type,
                            "status": status,
                            "error_message": None}
                    )

            except Exception as e:  
                status = "ERROR"
                error_message=getattr(e, 'message', str(e))
                logger.error(f"Error executing functional check with {rule_id} on object - {object_name}: {error_message}")
                functional_results.append(
                        {
                            "rule_id": rule_id,
                            "name": name,
                            "object": object_name,
                            "rule_type": rule_type,
                            "status": status,
                            "error_message": error_message}
                    )
        return functional_results

    def _get_summary(self,results):
        """
        Return summary of the checks with total, passed, failed and error counts
        """
        total_checks = len(results)
        passed_checks = len([r for r in results if r['status'] == 'PASS'])
        failed_checks = len([r for r in results if r['status'] == 'FAIL'])
        error_checks = len([r for r in results if r['status'] == 'ERROR'])
        return {
            "total": total_checks,
            "passed": passed_checks,
            "failed": failed_checks,
            "error": error_checks
        }
        
    
    def config_loader(self):
        """
            Read the yaml file and returns it
        """
        with open(self.yaml_path,'r') as conf:
            return yaml.safe_load(conf)


    def write_results_csv(self,results):
        """
        Write the results to a csv file in the local directory
        """
        rows=[]

        for category in ["sanity","Functional"]:
            rows.extend(
                [{'category':category,**item}for item in results[category]]
            )

        df=pd.DataFrame(rows)
        df.to_csv("test_result.csv",index=False)

        logger.info(f"Results written to test_result.csv")
       

        
        

if __name__ == "__main__":
    """
    Main function to execute the testing framework
    """

    load_dotenv()
    yaml_path = "Framework/config.yaml"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    logger=logging.getLogger(("snowpark_testing"))

    logging.getLogger("snowflake").setLevel(logging.WARNING)
    logging.getLogger("snowflake.snowpark").setLevel(logging.WARNING)

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
        snowpark_testing = SnowparkValidationRunner( session, yaml_path  )
        results=snowpark_testing.execute()
        #logger.info(f"results: {results}")

    # Export the output as csv format 
        snowpark_testing.write_results_csv(results)


    except Exception as e :
        logger.info(f"Script aborted due to this error {type(e)}:  {e}")

import requests
import json
import time
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery
from google.cloud import secretmanager
import logging
import requests
import json
import csv
import datetime
from datetime import date
from datetime import datetime,timezone
import time
import sys
import re
import os
from processrequest import Processapirequest

souobj = None
class Varicentapi(object):

    def __init__(self):
        global souobj
        self.environment=''
        self.model=''
        self.sleep_time=''
        self.retries=0
        self.global_status_url=''
        self.folder_details_url=''
        self.scheduler_url=''
        self.environment_url=''
        self.headers=''
        self.audit_url=''
        self.validation_data_url=''
        self.payload={}
        self.folder_id=''
        self.parent_folder=''
        self.project=''
        self.query=''
        self.module=''
        self.pipeline_name=''
        self.file_name=''
        self.input_tables=''
        self.audit_log_table=''
        self.error_log_table=''
        self.source_validations_table=''
        self.orchestration_dataset=''
        self.target_dataset=''
        self.varicent_configuration=''
        self.key_name=''
        self.user_name=''
        self.serviceaccount_filepath=''
        self.source_details=''
        self.error_codes=''
        self.current_table_name=''
        self.ingestion_time=''
        self.connection_retries=0
        self.table_response=''
        self.bq_connection_retries=0
        souobj = Processapirequest()
        

    def _access_secret_version(self, project_id, secret_id, version_id):
    
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = "projects/{}/secrets/{}/versions/{}".format(project_id, secret_id, version_id)

            if project_id in (None, ' ', '') or secret_id in (None, ' ', '') or version_id in (None, ' ', ''):
                raise ValueError
            response = client.access_secret_version(name=name)
            secret_content = response.payload.data.decode('UTF-8')

        except ValueError:
            secret_content = ValueError
            print("coming out from the access secret version ")
            self.make_audit_log_entry(self.source_details + ": " +  "Error in fetching password from the secret key management")
            raise sys.exit(1)

        return secret_content if secret_content is not None else None

    def get_environment_details(self,table_detail_values,file_name,ingestion_time):

        try:
            with open("config.json") as json_data_file:
                varicent_env_details = json.load(json_data_file)

            varicent_config_details = varicent_env_details['environment-details']

            self.project = varicent_config_details['project']
            self.module = varicent_config_details['module_names'][1]
            self.audit_log_table = varicent_config_details['audit_log_table']
            self.error_log_table = varicent_config_details['error_log_table']
            self.orchestration_dataset = varicent_config_details['orchestration_dataset']
            self.varicent_configuration = varicent_config_details['varicent_configuration']
            self.key_name = varicent_config_details['key_name']
            self.user_name = varicent_config_details['user_name']
            self.environment = varicent_config_details['environment']
            self.serviceaccount_filepath =  varicent_config_details['serviceaccount_filepath']
            self.target_dataset =  varicent_config_details['target_dataset']
            self.source_validations_table =  varicent_config_details['validations_table']
            self.pipeline_name = varicent_config_details['pipeline_name']
            self.file_name=file_name
            self.input_tables=table_detail_values
            self.ingestion_time=ingestion_time
            self.source_details='Source Validations'
            os.environ['GOOGLE_APPLICATION_CREDENTIALS']  = self.serviceaccount_filepath
            client = bigquery.Client()
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            query = "select * from " + self.project + "." + self.orchestration_dataset + "." + self.varicent_configuration + " where environment = '" + self.environment + "'"

            query_job = client.query(query,job_config=job_config)

            for env_details in query_job:

                self.environment=env_details['environment']
                self.model=env_details['model']
                self.sleep_time=env_details['sleeptimeinseconds']
                self.retries=int(env_details['maxretries'])
                self.global_status_url=env_details['globalactionstatusurl']
                self.folder_details_url=env_details['folderdetailsurl']
                self.scheduler_url=env_details['schedulerurl']
                self.environment_url=env_details['environmenturl']
                self.audit_url=env_details['auditlogurl']
                self.parent_folder=env_details['parentfolder']
                self.login_url=env_details['loginurl']
                self.validation_data_url=env_details['validationdataurl']
                
            souobj.get_bq_environment_details()
            
            try:
                bearer_token = self.get_token_details(self.environment_url,self.login_url,self.user_name)

                varicent_token='Bearer '+bearer_token
                self.headers =  {'Content-Type': 'application/json',
                                'Model': self.model,
                                'Authorization': varicent_token}
            except Exception as errormsg:
                pass

            self.make_audit_log_entry(self.source_details + ": " +  "Successfully fetched varicent APIConfiguration details")

        except Exception as errormsg:
            self.make_error_log_entry("3000","Error while extracting varicent APIConfiguration details",
            "Invocation of Validationdata APIConfiguration has been responded with error message " )
            #raise Exception("Exception: ", str(error_message))
            print("coming out from the environment loop")
            self.make_audit_log_entry(self.source_details + ": " +  "Error occured in setting up the environment details")
            raise sys.exit(1)

    def get_token_details(self,environment_url,login_url,username):
    
        try:
            password = self._access_secret_version(self.project, self.key_name, 'latest')

            if(password):
                self.make_audit_log_entry(self.source_details + ": " + "Password has been fetched from secret management successfully")

            payload={"email" : username,"password" : password}
            counter = 0
            token_url = environment_url+login_url
            response = souobj.apiRequest(token_url,'POST',payload,self.headers)
            #response = requests.post(token_url,data=payload)
            
            while (response.status_code != 200):
                counter = counter + 1
                print("Response is not 200 after "+str(counter) + " while fetching varicent token details" )
                if (counter == self.retries):
                    self.make_error_log_entry("3001","Reached maximum retries while fetching token details from varicent environment",
                    "API Invocation has reached to maximum retires "  + str(counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.source_details + ": " + "Reached Maximum retries while fetching the token details")
                    raise sys.exit(1)
                    
                time.sleep(int(self.sleep_time))
                response = souobj.apiRequest(token_url,'POST',payload,self.headers)
                
            response_final = json.loads(response.text)
            varicent_token = response_final['token']
            
            if not varicent_token :
                print("error in fetching token details from varicent environment")
                print("coming out from the get token details ")
                raise sys.exit(1)
            self.make_audit_log_entry(self.source_details + ": " + "Varicent bearer token has been fetched successfully")
            
            print("Varicent bearer token has been fetched successfully")    
            return varicent_token

        except SystemExit as errormsg:
            self.make_error_log_entry("3002","Error occured while getting token details",
            "Error occured while getting token details from varicent environment ")
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the folder id  exception loop ")
            self.make_audit_log_entry(self.source_details + ": " +  "Error occured while fetching token details from environment")
            raise sys.exit(1)

    def get_table_details(self):
        
        try:
            input_values = self.input_tables
            print("Table list for data refresh "+input_values)
            table_names = input_values
        
            for table_name in table_names.split(','):
                print("1")
                self.current_table_name = table_name
                self.bq_connection_retries=0
                counter = 0
                table_details_url=  self.environment_url + self.validation_data_url + table_name + '/inputforms/0/data?offset=0&limit=1000'
                print("1 table_details_url="+table_details_url)
                if (table_name.lower() == "cfgeffectivepayee"):
                    table_details_url = table_details_url + "&fields=EnterpriseID,EffStart_,EffEnd_"
                elif (table_name.lower() == "cfgemployeestatus"):
                    table_details_url = table_details_url + "&fields=EnterpriseID,EffStart_,EffEnd_,EmployeeOnBoardingStatusEventType"
                elif (table_name.lower() == "cfgallnaspv1" or table_name.lower() == "cfgallnasp"):
                    table_details_url = table_details_url + "&fields=NASPID,EffStart_,EffEnd_"
                elif (table_name.lower() == "cfgplaninfo"):
                    table_details_url = table_details_url + "&fields=CompPlanID,EffStart_,EffEnd_,TypeGroup"

                print("1a table_details_url="+table_details_url)
            

                #table_data_response = requests.get(table_details_url,headers=self.headers)
                table_data_response = souobj.apiRequest(table_details_url,'GET',self.payload,self.headers)
                print("1c table_data_response="+str(table_data_response))
                
                if not json.loads(table_data_response.text)["data"]:
                    print("Data is not available for the table " + table_name + " in varicent environment")
                    
                    self.delete_gcp_tabledata()

                while (table_data_response.status_code != 200):
                    print("2")
                    counter = counter + 1
                    print("counter="+str(counter))
                    time.sleep(int(self.sleep_time))
                    print("after sleep")
                    if (counter == 2):
                        print("counter=2")
                        self.make_audit_log_entry(self.source_details + ": " + "Validation data refresh is " + str(table_data_response.status_code) + ", waiting for the response code 200")
                        print("after auditlogentr 2")
                    if (counter == self.retries):
                        print("3")
                        self.make_error_log_entry("3002","Reached maximum retries while refreshing validation data",
                        "Validation data refresh API call has reached to maximum retires "  + str(counter) + " and is stopping")
                        print("4")
                        print("coming out from the get table details self retries")
                        self.make_audit_log_entry(self.source_details + ": " +  "Reached maximum retries while refreshing validation data")
                        raise sys.exit(1)
                    #table_data_response = requests.get(table_details_url,headers=self.headers)
                    table_data_response = souobj.apiRequest(table_details_url,'GET',self.payload,self.headers)
                    print("4 table_data_response="+str(table_data_response))

                totalRows = int(table_data_response.headers['totalRows'])
                print("4 totalRows="+str(totalRows))
                rc = 0
                
                while (rc < totalRows):
                    print("4 rc="+str(rc)+" totalRows="+str(totalRows))
                    table_details_response = json.loads(table_data_response.text)
                    print("4 table_details_response="+str(table_details_response))
                    if (rc == 0):
                        print("5")
                        self.delete_gcp_tabledata()
                        print("6")
                        rc = rc + self.make_tablevalue_entry(table_details_response)
                        print("6 rc="+str(rc))
                    else:
                        rc = rc + self.make_tablevalue_entry(table_details_response)
                        print("7 rc="+str(rc))
                    table_details_url = self.environment_url + self.validation_data_url + table_name + '/inputforms/0/data?offset=' + str(rc) + '&limit=1000'
                    print("7 table_details_url="+str(table_details_url))
                    if (table_name.lower() == "cfgeffectivepayee"):
                        table_details_url = table_details_url + "&fields=EnterpriseID,EffStart_,EffEnd_"
                    elif (table_name.lower() == "cfgemployeestatus"):
                        table_details_url = table_details_url + "&fields=EnterpriseID,EffStart_,EffEnd_,EmployeeOnBoardingStatusEventType"
                    elif (table_name.lower() == "cfgallnaspv1" or table_name.lower() == "cfgallnasp"):
                        table_details_url = table_details_url + "&fields=NASPID,EffStart_,EffEnd_"
                    elif (table_name.lower() == "cfgplaninfo"):
                        table_details_url = table_details_url + "&fields=CompPlanID,EffStart_,EffEnd_,TypeGroup"
                    print("7a table_details_url="+str(table_details_url))
                    #table_data_response = requests.get(table_details_url,headers=self.headers)
                    table_data_response = souobj.apiRequest(table_details_url,'GET',self.payload,self.headers)
                    print("7a table_data_response="+str(table_data_response))
                    while (table_data_response.status_code != 200):
                        print("9")
                        counter = counter + 1
                        print("9 counter="+str(counter))
                        time.sleep(int(self.sleep_time))
                        print("10 after sleep")
                        if (counter == 2):
                            print("10 counter = 2")
                            self.make_audit_log_entry(self.source_details + ": " + "Validation data refresh is " + str(table_data_response.status_code) + ", waiting for the response code 200")
                            print("11 after audit")
                        if (counter == self.retries):
                            print("12")
                            self.make_error_log_entry("3003","Reached maximum retries while refreshing validation data",
                            "Validation data refresh API call has reached to maximum retires "  + str(counter) + " and is stopping")
                            print("13 after audit about to exit")
                            print("coming out from the get table details self retries 2")
                            self.make_audit_log_entry(self.source_details + ": " +  "Reached maximum retries while refreshing validation data")
                            raise sys.exit(1)
                        #table_data_response = requests.get(table_details_url,headers=self.headers)
                        table_data_response = souobj.apiRequest(table_details_url,'GET',self.payload,self.headers)
                        #print("14 table_data_response="+str(table_data_response))

                    print(str(rc)+" of " + str(totalRows) + " inserted")
                self.make_audit_log_entry(self.source_details + ": " + "Total " + str(totalRows) + " rows inserted for the table " + table_name.lower() )

        except SystemExit as errormsg:
            self.make_error_log_entry("3004","Exception occured while fetching table validation data" + str(self.current_table_name),
            "Exception occured while making API Call to fetch table validation data  " )
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the get table details exception loop")
            self.make_audit_log_entry(self.source_details + ": " +  "Error occured while fetching table validation data" + str(self.current_table_name))
            raise sys.exit(1)
    
    def delete_gcp_tabledata(self):
    
        try:
        
            validation_table_name = self.current_table_name
            code_type = validation_table_name.lower()
            client = bigquery.Client()
            table = client.get_table("{}.{}.{}".format(self.project, self.target_dataset, self.source_validations_table))
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            query_delete="delete  " + self.project + "." + self.target_dataset + "." + self.source_validations_table +  " where codetype = '" + code_type  + "'"
            print("Deleting existing validation data for Varicent subject "+validation_table_name)
            query_job = client.query(query_delete,job_config=job_config)
            
        except Exception as errormsg:
        
            self.make_error_log_entry("3005","Exception occured while deleting the data",
            "Exception occured while deleting the table data from bigquery" + str(self.current_table_name))
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the delete gcp tabledata exception loop")
            self.make_audit_log_entry(self.source_details + ": " +  "Exception occured while deleting the sourcevalidation table data from bigquery" + str(self.current_table_name))
            raise sys.exit(1)
        
        
    
    
    def make_tablevalue_entry(self,table_details_response):
    
        try:
            self.table_response=table_details_response
            validation_table_name = self.current_table_name
            code_type = validation_table_name.lower()
            client = bigquery.Client()
            table = client.get_table("{}.{}.{}".format(self.project, self.target_dataset, self.source_validations_table))
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            #print("Now inserting updated validation data for Varicent subject "+validation_table_name)
            #self.make_audit_log_entry(self.source_details + ": " + "Now inserting updated validation data for Varicent subject " + validation_table_name ) 
            rowcnt = 0
            firstrow = 'Y'
            query_insert = \
              "insert into " + self.project + "." + self.target_dataset + "." + self.source_validations_table + " " \
              + "(effectivestartdate, effectiveenddate, codetype, codevalue, codedescription) values "
            code_description = ''

            for table_values in table_details_response['data']:
                if code_type == "cfgeffectivepayee" or code_type == "cfgallnaspv1" or code_type == "cfgallnasp": 
                    code_description = str(table_values[0])
                    eff_start_date = table_values[1][0:10]
                    eff_end_date = table_values[2][0:10]
                elif code_type == "cfgplaninfo" or code_type == "cfgemployeestatus":
                    code_description = str(table_values[3])
                    eff_start_date = table_values[1][0:10]
                    eff_end_date = table_values[2][0:10]
                else:            
                    code_description = str(table_values[1])
                    eff_start_date = '1753-01-01'
                    eff_end_date = '9998-12-31'
                #if firstrow != "Y":
                    #query_insert = query_insert + ", "
                    #firstrow = "N"
                query_insert = query_insert \
                  + "(safe_cast('" + eff_start_date + "' as date format 'yyyy-mm-dd'), " \
                  + "safe_cast('" + eff_end_date + "' as date format 'yyyy-mm-dd'), " \
                  + "'" + code_type.replace("'","\\'") + "', " \
                  + "'" + table_values[0].replace("'","\\'") + "', " \
                  + "'" + code_description.replace("'","\\'") + "')"
                  
                rowcnt = rowcnt + 1
                if rowcnt == len(table_details_response['data']):
                    query_insert = query_insert
                else:
                    query_insert = query_insert + ","
                
            query_job = client.query(query_insert,job_config=job_config)

            results = query_job.result()

            return rowcnt
            
        except Exception as errormsg:
            rowcnt=0
            print("coming into exception loop in getsourcevalidations table entry")   
            self.bq_connection_retries = self.bq_connection_retries + 1  
            if (self.bq_connection_retries == 1):
                self.make_audit_log_entry(self.source_details + ": " +  "Exception occured while entering table value entries in the bigquery for " + str(self.current_table_name))
                print("Exception occured and retrying again")
                
            if (self.bq_connection_retries == self.retries):
                print("maximum retries reached")
                self.make_error_log_entry("3006","Reached maximum retries while making table entries in the Bigquery",
                "Reached maximum retries in exception block while entering values into bigquery table for pltable " + str(self.current_table_name) )
                self.make_audit_log_entry(self.source_details + ": " +  "Reached maximum retries in exception block while entering table value entries in the bigquery for " + str(self.current_table_name))
                raise sys.exit(1) 
            
            time.sleep(int(self.sleep_time))
            rowcnt = self.make_tablevalue_entry(self.table_response)
            return rowcnt
            
            
    def make_audit_log_entry(self,audit_message):

        try:
        
            event_time_in_utc = datetime.utcnow().replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
            ''' client = bigquery.Client()
            table = client.get_table("{}.{}.{}".format(self.project, self.orchestration_dataset, self.audit_log_table))
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            query="SELECT IFNULL(MAX(auditcode),0) + 1 as auditcode FROM " + self.project + "." + self.orchestration_dataset + "." + self.audit_log_table
            query_job = client.query(query,job_config=job_config)

            for audit_details in query_job:
                audit_code=(audit_details['auditcode']) '''
                
            ingestion_header =  "insert into " + self.project + "." + self.orchestration_dataset + "." + self.audit_log_table + " " \
                          + "(module, auditmessage, pipelinename, filename, ingestiontime, eventdatetime) values "
            #audit_code = "SELECT IFNULL(MAX(auditcode),0) + 1 as auditcode FROM " + self.project + "." + self.orchestration_dataset + "." + self.audit_log_table
            
            if self.query:
                self.query = self.query \
                             + "("  + "'" + self.module + "', '''" + audit_message + "''', '" + self.pipeline_name + "', '" + self.file_name + "', '" + self.ingestion_time + "', '" + event_time_in_utc + "')" + ","
            else:
                self.query = ingestion_header \
                             + "("  + "'" + self.module + "', '''" + audit_message + "''', '" + self.pipeline_name + "', '" + self.file_name + "', '" + self.ingestion_time + "', '" + event_time_in_utc + "')" + ","

            #query_job = client.query(query,job_config=job_config)

        except Exception as errormsg:
            print("Exception: Exception occured in audit log entry")
            print("coming out from the audit log entry loop") 
            self.make_audit_log_entry(self.source_details + ": " +  "Exception occured while creating audit log entry records")
            raise sys.exit(1)
            
    def make_audit_table_entry(self):
        try:
        
            client = bigquery.Client()
            table = client.get_table("{}.{}.{}".format(self.project, self.orchestration_dataset, self.audit_log_table))
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            query = self.query.strip(',')
            query_job = client.query(query,job_config=job_config)
            results = query_job.result()
        except Exception as errormsg:
            print("Exception: Exception occured in audit table entry")
            print("coming out from the audit table entry loop") 
            self.make_audit_log_entry(self.source_details + ": " +  "Exception occured while making entries in the table")
            raise sys.exit(1)

    def make_error_log_entry(self,error_code,error_message,error_details_message):
        try:
            event_time_in_utc = datetime.utcnow().replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
            
            client = bigquery.Client()
            table = client.get_table("{}.{}.{}".format(self.project, self.orchestration_dataset, self.error_log_table))
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)

            query="insert into " + self.project + "." + self.orchestration_dataset + "." + self.error_log_table + " " \
              + "(errorcode, errormessage, errordetails, pipelinename, filename, ingestiontime, errordatetime, severity) values " \
              + "('" + error_code + "', '" + error_message + "', '" + error_details_message + "', '" + self.pipeline_name + "', '" + self.file_name + "', '" + self.ingestion_time + "', '" + event_time_in_utc + "', 0)" 
              
            query_job = client.query(query,job_config=job_config)

        except Exception as errormsg:
            print("Exception: Exception occured in error log entry")
            print("coming out from the errorlog table entry loop") 
            self.make_audit_log_entry(self.source_details + ": " +  "Exception occured while making error log entry ")
            raise sys.exit(1)



def main():
    try:

        if len(sys.argv) != 4:
            sys.exit("Please provide enough arguments")

        table_detail_values = sys.argv[1]
        file_name = sys.argv[2]
        ingestion_time = sys.argv[3]

        print("Refreshing Validation Data via API pull from Varicent")
        varobj = Varicentapi()
        varobj.get_environment_details(table_detail_values,file_name,ingestion_time)
        varobj.get_table_details()
        varobj.make_audit_table_entry()
    
    except SystemExit as errormsg:
        logging.exception(print("Exception :Exception in the program system exit " + str(errormsg) ))
        varobj.make_audit_table_entry()
        raise sys.exit(1)
        
        
    except Exception as errormsg :
        logging.exception(print("Exception :Exception in the getsourcevalidation api call program") )
        varobj.make_audit_table_entry()
        raise sys.exit(1)

    return "Success"

if __name__ == '__main__':
    main()

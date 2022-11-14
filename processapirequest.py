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
from google.oauth2 import service_account
import traceback

class Processapirequest(object):

    def __init__(self):
    
        self.environment=''
        self.model=''
        self.sleep_time=''
        self.retries=0
        self.global_status_url=''
        self.folder_details_url=''
        self.scheduler_url=''
        self.environment_url=''
        self.audit_url=''
        self.payload={}
        self.folder_id=''
        self.parent_folder=''
        self.project=''
        self.module=''
        self.pipeline_name=''
        self.file_name=''
        self.audit_log_table=''
        self.error_log_table=''
        self.orchestration_dataset=''
        self.varicent_configuration=''
        self.key_name=''
        self.user_name=''
        self.serviceaccount_filepath=''
        self.error_codes=''
        self.query=''
        self.globalstarttime=''
        self.globalStopTime=''
        self.log_count=0
        self.ingestion_time=''
        self.folder_name=''
        self.connection_retries=0
        self.import_counter=0
        self.folder_retries=0
        self.url=''
        self.verb=''
        self.payload=''
        self.headers=''
        self.check=0
        self.exception_occured = False
        self.url_test=''
    def get_bq_environment_details(self):

        try:
            
            with open("config.json") as json_data_file:
                varicent_env_details = json.load(json_data_file)
            
        
            varicent_config_details = varicent_env_details['environment-details']
        
            self.project = varicent_config_details['project']
            self.module = varicent_config_details['module_names'][0]
            self.audit_log_table = varicent_config_details['audit_log_table']
            self.error_log_table = varicent_config_details['error_log_table']
            self.orchestration_dataset = varicent_config_details['orchestration_dataset']
            self.varicent_configuration = varicent_config_details['varicent_configuration']
            self.key_name = varicent_config_details['key_name']
            self.user_name = varicent_config_details['user_name']
            self.environment = varicent_config_details['environment']
            self.serviceaccount_filepath =  varicent_config_details['serviceaccount_filepath']
            self.pipeline_name = varicent_config_details['pipeline_name']
            
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
            
            
        except Exception as errormsg:
            print("Exception occured while fetching environment details")
            raise sys.exit(1)
            
    
    def apiRequest(self,p_url, p_verb, p_payload, p_headers):
       
        #print("url="+p_url+" verb="+p_verb+" payload="+str(p_payload)+" headers="+str(p_headers))
        try:
            self.url=p_url
            self.verb=p_verb
            self.payload=p_payload
            self.headers=p_headers
    
            response = requests.request(p_verb, p_url, headers=p_headers, data=p_payload)
             
        except requests.exceptions.RequestException as e:
            print("coming into exception loop")   
            self.exception_occured = True
            self.connection_retries = self.connection_retries + 1  
            if (self.connection_retries == 1):
                print("Exception occured and retrying again")
                
            if (self.connection_retries == self.retries):
                print("maximum retries reached")
                raise sys.exit(1) 
            
            time.sleep(int(self.sleep_time))
            response = self.apiRequest(self.url,self.verb,self.payload,self.headers)
            
        self.exception_occured = False
        return response
    
   

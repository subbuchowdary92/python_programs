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
from processrequest import Processapirequest
# generic http post w retry loop if response not 200

apiobj = None
class Varicentapi(object):

    def __init__(self):
        global apiobj
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
        self.headers=''
        self.log_count=0
        self.ingestion_time=''
        self.folder_name=''
        self.connection_retries=0
        self.import_counter=0
        self.folder_retries=0
        self.global_action_counter=0
        self.global_counter=0
        apiobj = Processapirequest()
        self.check = 0
        
    def _access_secret_version(self, project_id, secret_id, version_id):
        try:
            print("debug1")
            client = secretmanager.SecretManagerServiceClient()
            name = "projects/{}/secrets/{}/versions/{}".format(project_id, secret_id, version_id)
            
            if project_id in (None, ' ', '') or secret_id in (None, ' ', '') or version_id in (None, ' ', ''):
                raise ValueError
            response = client.access_secret_version(name=name)
            secret_content = response.payload.data.decode('UTF-8')

        except ValueError:
            secret_content = ValueError
            print("coming out from the access secret version ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error in fetching the secret key")
            raise sys.exit(1)

        return secret_content if secret_content is not None else None

    def get_environment_details(self,file_name,pipeline_name,folder_name,ingestion_time):

        try:
            print("debug2")
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
            self.file_name=file_name
            self.folder_name = folder_name
            self.ingestion_time = ingestion_time
            
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
            
            apiobj.get_bq_environment_details()
            
            utc_time = datetime.utcnow().replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')
            utc_time_withzone = utc_time[:-4] + 'Z'
            self.globalstarttime = utc_time_withzone
            
            try:
                bearer_token = self.get_token_details(self.environment_url,self.login_url,self.user_name)

                varicent_token='Bearer '+bearer_token
                self.headers =  {'Content-Type': 'application/json',
                                 'Model': self.model,
                                'Authorization': varicent_token}
            except Exception as errormsg:
                pass

            
            self.make_audit_log_entry(self.folder_name + ": " +  "Successfully fetched varicent APIConfiguration details")
            
        except Exception as errormsg:
            self.make_error_log_entry("1000","Error while extracting varicent APIConfiguration details",
            "Invocation of varicent APIConfiguration has been responded with error message")
            #raise Exception("Exception: ", str(error_message))
            print("coming out from the Environment details ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured in setting up the environment details")
            #raise sys.exit(1)
            

    def get_token_details(self,environment_url,login_url,username):
        
        try:
            password = self._access_secret_version(self.project, self.key_name, 'latest')
            
            if(password):
                self.make_audit_log_entry(self.folder_name + ": " + "Password has been fetched from secret management successfully")
            payload={"email" : username,"password" : password}
            counter = 0
            token_url = environment_url+login_url
            response = apiobj.apiRequest(token_url,'POST',payload,self.headers)
  
            while (response.status_code != 200):
                counter = counter + 1
                print("Response is not 200 after "+str(counter) + " while fetching varicent token details" )
                if (counter == self.retries):
                    self.make_error_log_entry("1001","Reached maximum retries while fetching token details from varicent environment",
                    "API Invocation has reached to maximum retires "  + str(counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.folder_name + ": " + "Reached Maximum retries while fetching the token details")
                    raise sys.exit(1)
                    
                time.sleep(int(self.sleep_time))
                response = apiobj.apiRequest(token_url,'POST',payload,self.headers)
                
           
            response_final = json.loads(response.text)
            varicent_token = response_final['token']
          
            if not varicent_token :
                print("error in fetching token details from varicent environment")
                print("coming out from the get token details ")
                raise sys.exit(1)
            self.make_audit_log_entry(self.folder_name + ": " + "Varicent bearer token has been fetched successfully")
             
            print("Varicent bearer token has been fetched successfully")    
            return varicent_token

        except SystemExit as errormsg:
            self.make_error_log_entry("1001","Error occured while getting token details",
            "Error occured while getting token details from varicent environment ")
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the folder id  exception loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured while fetching token details from environment")
            raise sys.exit(1)
            
            
 
    def get_folder_id(self):

        try:
            print("debug folder details")
            #folder_detail_response = requests.get(self.environment_url+self.folder_details_url,headers=self.headers,data=self.payload)
            folder_detail_response = apiobj.apiRequest(self.environment_url+self.folder_details_url,'GET',self.payload,self.headers)
            
            folder_counter = 0
            while (folder_detail_response.status_code != 200):
                folder_counter = folder_counter + 1
                print("Response is not 200 after "+str(folder_counter) + "while fetching folder id details" )
                if (folder_counter == self.retries):
                    self.make_error_log_entry("1001","Reached maximum retries while fetching folderid details from varicent environment",
                    "API Invocation has reached to maximum retires "  + str(folder_counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.folder_name + ": " + "Reached Maximum retries while fetching the folder id details")
                    raise sys.exit(1)
                    
                time.sleep(int(self.sleep_time))
                folder_detail_response = apiobj.apiRequest(self.environment_url+self.folder_details_url,'GET',self.payload,self.headers)
                
            folder_details = json.loads(folder_detail_response.text)
            
            print("hit the folder id details url")
            for folders in folder_details:
                    if (folders['name']) == self.parent_folder:
                        subtasks = folders['childScheduleItems']
                        
                        for tasks in subtasks:
                            if (tasks['name']) == self.folder_name:
                                print("Scheduler folder details fetched successfully")
            
                                self.folder_id = tasks['id']
            print("printing folder id" + str(self.folder_id))    
            if self.folder_id:                    
                self.make_audit_log_entry(self.folder_name + ": " + "Folder ID details for " + self.folder_name + " extracted successfully from varicent environemnt")
            else:
                print("Could not able to fetch folder details for the folder name " + self.folder_name)
                self.make_audit_log_entry(self.folder_name + ": " + "Error in fetching folder details")
                print("coming out from the folder id not found loop ")
                raise sys.exit(1)          
                
        except SystemExit as errormsg:
            self.make_error_log_entry("1002","Exception occured while extracting FolderID details from scheduler",
            "Exception occured while fetching folder id details from the varicent environment ")
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the folder id  exception loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured while extracting FolderID details from scheduler")
            raise sys.exit(1)
            
            

    def get_global_action_status(self):

        
        try:
            global_action_status=True
            #global_action_response = requests.get(self.environment_url+self.global_status_url,headers=self.headers)
            global_action_response = apiobj.apiRequest(self.environment_url+self.global_status_url,'GET',self.payload,self.headers)
            
           
            while (global_action_response.status_code != 200):
                self.global_counter = self.global_counter + 1
                print("Response is not 200 after "+str(self.global_counter) + " while fetching global action details" )       
                if (self.global_counter == self.retries):
                    self.make_error_log_entry("1001","Reached maximum retries while fetching global action status",
                    "API Invocation has reached to maximum retires "  + str(self.global_counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.folder_name + ": " + "Reached Maximum retries while fetching global action status")
                    raise sys.exit(1)
                    
                time.sleep(int(self.sleep_time))
                global_action_response = apiobj.apiRequest(self.environment_url+self.global_status_url,'GET',self.payload,self.headers)
            
            global_action_status = global_action_response.json() 
            
            while global_action_status:
                self.global_action_counter = self.global_action_counter + 1
                
                if (self.global_action_counter == 1):
                    self.make_audit_log_entry(self.folder_name + ": " + "Global action status is " + str(global_action_status) + " and waiting for the availabitlity of environment to invoke scheduler job")
                if (self.global_action_counter == self.retries):
                    self.make_error_log_entry("1003","Reached maximum retries while fetching global action status",
                    "API Invocation has reached to maximum retires "  + str(self.global_action_counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.folder_name + ": " + "Reached Maximum retries while fetching the global action status")
                    raise sys.exit(1)
                    
                time.sleep(int(self.sleep_time))
                global_action_response = apiobj.apiRequest(self.environment_url+self.global_status_url,'GET',self.payload,self.headers)
                global_action_status = global_action_response.json()
                    
            else:
                print("Global action status is false and environment is available to load the data")
                self.make_audit_log_entry(self.folder_name + ": " + "Global action status is " + str(global_action_status) + " and ready to invoke scheduler job")
                #self.get_folder_id()
                #self.import_data()
                
            return global_action_status
                

        except SystemExit as errormsg:
            print("coming into request exception loop")
            self.make_error_log_entry("1004","Exception occured while getting global action status",
            "Exception occured while fetching global action status from varicent environment ")
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the get global action status loop ")
            self.make_audit_log_entry(self.folder_name + ": " + "Error occured while getting global action status from varicent environment")
            raise sys.exit(1)
            

    def import_data(self):

        try:
            import_data_url = self.environment_url+self.scheduler_url+str(self.folder_id)+"/run"
            #import_data_response = requests.post(import_data_url,headers=self.headers)
            import_data_response = apiobj.apiRequest(import_data_url,'POST',self.payload,self.headers)
            
            import_counter = 0
            while (import_data_response.status_code != 200):
                import_counter = import_counter + 1
                print("Response is not 200 after "+str(import_counter) + " while importing the data" )
                if (import_counter == self.retries):
                    self.make_error_log_entry("1001","Reached maximum retries while importing the data into varicent environment",
                    "API Invocation has reached to maximum retires "  + str(import_counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.folder_name + ": " + "Reached Maximum retries while importing the data into varicent environment")
                    raise sys.exit(1)
                print("sleeping for some time in import activities ")
                time.sleep(int(self.sleep_time))
                import_data_response = apiobj.apiRequest(import_data_url,'POST',self.payload,self.headers)

            print("invoked scheduler folder successfully")
            self.make_audit_log_entry(self.folder_name + ": " + "Invoked the scheduler job successfully to load the data into varicent environment")
            import_response = json.loads(import_data_response.text)
            completed_activities = import_response['completedactivities']
            live_activities = import_response['liveactivities']
            live_activities_response = self.get_live_activities_status(self.environment_url+live_activities)
            print("after get_live_activities_status="+str(live_activities_response))
            completed_activities_response = self.get_completed_activities_status(self.environment_url+completed_activities)

            completed_activites_details = json.loads(completed_activities_response.text)
            self.globalStopTime = completed_activites_details['time']
            audit_response=self.get_audit_logs()
            audit_details = json.loads(audit_response.text)
            audit_details_sorted = sorted(audit_details, key=lambda element: element['id'])
            temp_module = self.module
    
            for i,auditrow in enumerate(audit_details_sorted):
            
                if i+1 == len(audit_details_sorted):
                    self.module = auditrow['module']
                    #self.make_audit_log_entry(self.folder_name + ": " + auditrow['message'].replace("'","\\'"))
                    self.make_audit_log_entry(self.folder_name + ": " + str(" ".join(line.strip() for line in auditrow['message'].replace("'","\\'").splitlines())))
                    print("first auditrow message="+str(auditrow['message']))
                    error_status = self.finderrorinlog(auditrow['message'])
                    self.module = temp_module
                else:
                    self.module = auditrow['module']
                    self.make_audit_log_entry(self.folder_name + ": " + str(" ".join(line.strip() for line in auditrow['message'].replace("'","\\'").splitlines())))
                    #self.make_audit_log_entry(self.folder_name + ": " + auditrow['message'].replace("'","\\'"))
                    print("second auditrow message="+str(auditrow['message']))
                    error_status = self.finderrorinlog(auditrow['message'])

            if (error_status is not None):
                print("Error found in audit logs and coming out from the program")
                self.make_audit_log_entry(self.folder_name + ": " + "Error found in varicent audit log errors")
                raise sys.exit(1)
                    
            if (audit_response.status_code == 200):
                self.make_audit_log_entry(self.folder_name + ": " + "API calls for varicent environment has been finished successfully")
            #print(auditrow['userId']+"|"+auditrow['module']+"|"+auditrow['eventName']+"|"+auditrow['time']+"|"+auditrow['message']+"|"+str(auditrow['hasHistory']))

            print(" API calls executed successfully")
                
            

        except SystemExit as errormsg:
            self.make_error_log_entry("1005","Error occured while importing the data into varicent environment",
            "Error occured while triggering import folder in varicent environment ")
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the import data loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured while importing the data into varicent environment")
            raise sys.exit(1)
            

    def get_live_activities_status(self,live_activities_url):
    
        try:
            #activity_response = requests.get(live_activities_url,headers=self.headers,data=self.payload)
            activity_response = apiobj.apiRequest(live_activities_url,'GET',self.payload,self.headers)
            self.make_audit_log_entry(self.folder_name + ": " + "Checking the status of live activities")
            counter = 0

            while (activity_response.status_code == 200):
                activity_information = json.loads(activity_response.text)
                counter = counter + 1
                if counter == 1:
                    self.globalstarttime = activity_information['time']
                if counter == 2:
                   self.make_audit_log_entry(self.folder_name + ": " + "live activity status is " + str(activity_response.status_code) + " and waiting for the response other than 200")
                #print("sleeping for some time in live activities ")
                time.sleep(int(self.sleep_time))
                print("waiting for live activities to get completed")
                activity_response = apiobj.apiRequest(live_activities_url,'GET',self.payload,self.headers)
                print("activity_response="+str(activity_response))
            else:
                print("non-200")
                response_message = json.loads(activity_response.text)
                print("response_message="+str(response_message))
                self.make_audit_log_entry(self.folder_name + ": " + "Live activities has been completed with the status code " + str(activity_response.status_code))

            return activity_response

        except SystemExit as errormsg:

            self.make_error_log_entry("1006","Exception occured while checking the live activities status",
            "Exception occured while fetching status for the live activities response" )
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the live activities loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured while checking the live activities status")
            raise sys.exit(1)
            

    def get_completed_activities_status(self,completed_activities_url):

        try:
            #completed_activities_response = requests.get(completed_activities_url,headers=self.headers,data=self.payload)
            completed_activities_response = apiobj.apiRequest(completed_activities_url,'GET',self.payload,self.headers)
            self.make_audit_log_entry(self.folder_name + ": " + "Checking the status of completed activities")
            counter = 0
            
            while (completed_activities_response.status_code != 200):
                counter = counter + 1
                if (counter == 2):
                    self.make_audit_log_entry(self.folder_name + ": " + "completed activities status is " + str(completed_activities_response.status_code) + " and waiting for the response code 200")
                if (counter == self.retries):
                    self.make_error_log_entry("1007","Reached maximum retries while checking completed activities status",
                    "Completed activities API Invocation has reached to maximum retires "  + str(counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.folder_name + ": " + "Reached maximum retries while checking completed activities status")
                    raise sys.exit(1)
                time.sleep(int(self.sleep_time))
                completed_activities_response = apiobj.apiRequest(completed_activities_url,'GET',self.payload,self.headers)
            else:
                response_message = json.loads(completed_activities_response.text)
                self.make_audit_log_entry(self.folder_name + ": " + "Completed activities has been finished with the status code " + str(completed_activities_response.status_code))

            return completed_activities_response

        except SystemExit as errormsg:
            self.make_error_log_entry("1008","Exception occured while checking the completed activities status",
            "Exception occured while checking completed activities status response" )
            #raise Exception("Exception: ", str(errormsg))
            print("coming out from the completed activities status loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured while checking the completed activities status")
            raise sys.exit(1)
            

    def get_audit_logs(self):

        try:
            audit_log_url=self.environment_url+self.audit_url+"?filter=time=["+self.globalstarttime+"\,"+self.globalStopTime+"]"+";userId=" + self.user_name
            print(audit_log_url)
            #audit_log_response = requests.get(audit_log_url,headers=self.headers,data=self.payload)
            audit_log_response = apiobj.apiRequest(audit_log_url,'GET',self.payload,self.headers)
            self.make_audit_log_entry(self.folder_name + ": " + "Filtering the audit logs with start time " + str(self.globalstarttime) + " and stop time " + str(self.globalStopTime))

            counter = 0
            print("fetching audit logs from varicent environment")
            while (audit_log_response.status_code != 200):
                counter = counter + 1
                time.sleep(int(self.sleep_time))
                if (counter == 2):
                    self.make_audit_log_entry(self.folder_name + ": " + "Audit log API call response code is " + str(audit_log_response.status_code) + " and waiting for the response code 200")
                if (counter == self.retries):
                    self.make_error_log_entry("1009","Reached maximum retries while checking audit log status",
                    "Audit log API Invocation has reached to maximum retires "  + str(counter) + " and stopped executing further steps")
                    self.make_audit_log_entry(self.folder_name + ": " + "Reached maximum retries while checking audit log status")
                    raise sys.exit(1)
                audit_log_response = apiobj.apiRequest(audit_log_url,'GET',self.payload,self.headers)
            else:
                self.make_audit_log_entry(self.folder_name + ": " + "Audit logs has been fetched successfully")
            return audit_log_response
        
        except SystemExit as errormsg:
            self.make_error_log_entry("1010","Exception occured while extracting the audit logs from varicent environment",
            "Exception occured while fetching audit logs from varicent environment" )
            #raise Exception("Exception: ", str(errormsg)),
            print("coming out from the audit logs loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured while extracting the audit logs from varicent environment")
            raise sys.exit(1)
            

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
            
            self.log_count = self.log_count + 1
            if self.query:
                self.query = self.query \
                             + "("  + "'" + self.module + "', '''" + audit_message + "''', '" + self.pipeline_name + "', '" + self.file_name + "', '" + self.ingestion_time + "', '" + event_time_in_utc + "')" + ","
            else:
                self.query = ingestion_header \
                             + "("  + "'" + self.module + "', '''" + audit_message + "''', '" + self.pipeline_name + "', '" + self.file_name + "', '" + self.ingestion_time + "', '" + event_time_in_utc + "')" + ","

            #query_job = client.query(query,job_config=job_config)

        except Exception as errormsg :
            print("Exception: Exception occured in auditlog entry" )
            print("coming out from the audit log entry loop")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured while preparing auditlog entries")
            raise sys.exit(1)
            
            
    def make_audit_table_entry(self):
        try:
            print("coming inside the audit table entry") 
            client = bigquery.Client()
            table = client.get_table("{}.{}.{}".format(self.project, self.orchestration_dataset, self.audit_log_table))
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            query = self.query.strip(',')
            query_job = client.query(query,job_config=job_config)
            results = query_job.result()
        except Exception as errormsg :
            print("Exception: Exception occured in audit table entry")
            print("coming out from the audit table entry loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Error occured in while making entries into audit table")
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
            results = query_job.result()

        except Exception as errormsg :
            print("Exception: Exception in error log entry" )
            print("coming out from the error log entry loop ")
            self.make_audit_log_entry(self.folder_name + ": " +  "Exception in error log entry")
            raise sys.exit(1)
            
            
    def finderrorinlog(self,input_string):
        try:
            error_details = re.search(r"\berrors\b|\bError\b", input_string)
            return error_details
        except Exception as errormsg :
            print("Exception: Exception in finding error")
            print("coming out from the find error log loop")
            self.make_audit_log_entry(self.folder_name + ": " +  "Exception occured while checking error in auditlog")

   

def main():
    try:

        if len(sys.argv) != 5:
            sys.exit("Please provide enough arguments")
            
            
        file_name = sys.argv[1]
        pipeline_name = sys.argv[2]
        folder_name = sys.argv[3]
        ingestion_time = sys.argv[4]

        print("Starting API calls for varicent environment")
        varobj = Varicentapi()
        print("debug main 1")
        varobj.get_environment_details(file_name,pipeline_name,folder_name,ingestion_time)
        global_status = varobj.get_global_action_status()
        print("debug main 2")
        if not global_status:
            varobj.get_folder_id()
            print("debug main 3")
            varobj.import_data()
            print("debug main 4")
        varobj.make_audit_table_entry()
        print("debug main 5")
    
    except SystemExit as errormsg:
        logging.exception(print("Exception :Exception in the program system exit " + str(errormsg) ))
        varobj.make_audit_table_entry()
        raise sys.exit(1)
        
    except Exception as errormsg :
        print("coming into exception loop")
        logging.exception(print("Exception :Exception in the varicent api call program") )
        varobj.make_audit_table_entry()
        raise sys.exit(1)

    return "Success"

if __name__ == '__main__':
    main()

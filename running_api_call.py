import pysftp
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery
import logging
import requests
import json
import csv
import datetime
from datetime import date
from datetime import datetime


class APICall():

    def __init__(self):
        bucket_name='vz-poc-gcs1'
        self.table_schema = {
          'name': 'id',
          'type': 'STRING',
          'mode': 'NULLABLE'
          }, {
          'name': 'userType',
          'type': 'STRING',
          'mode': 'NULLABLE'
          }, {
          'name': 'userId',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'module',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'eventName',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'time',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'message',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'hasHistory',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'revisionStart',
          'type': 'STRING',
          'mode': 'NULLABLE'
          },{
          'name': 'revisionEnd',
          'type': 'STRING',
          'mode': 'NULLABLE'
          }
    
    def extract_gcs_files(self):
        client = storage.Client()
        bucket_name="vz_practice"
        folder="Archive/"
        bucket = client.get_bucket(bucket_name)
        file="vz"
        delimiter='/'
        new_file_name= "VzDIPOC/Archive/Vz_Test_Payee_To_Plan" +  datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
        blob = bucket.blob("Archive/Vz_Test_Payee_To_Plan.csv")
        #blobs=list(bucket.list_blobs(prefix=folder, delimiter=delimiter))
        cnopts = pysftp.CnOpts()
        line_count = 0
        dest_good_file = '/tmp/tmpgood.csv'
        cnopts.hostkeys = None
        print("coming here atleast")
        with  pysftp.Connection(host='ftp.spm.ibmcloud.com', username='DeloitteTraining', password='Varicent*123',
                                                port=22,
                                                cnopts=cnopts) as sftp:
                        logging.info("Connection succesfully established ... ")
                        with sftp.open("/Data/Vz_Test_Payee_To_Plan.csv", 'w', 32768) as f:
                            blob.download_to_file(f)
                        self.post_api_call()
    def post_api_call(self):

        #  from_date = str(dates[0])
        #  to_date = str(dates[1])
        bearer_token='icm-J0oPxzJ/ryOobrcy+ynQ6UyoHKnNfxo2PLJl9PeZvUg='
        headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + bearer_token,
                'Model': 'DeloitteTraining'
                }
        api_url='https://spm.varicent.com/api/v1/rpc/imports/91/run'
        r = requests.post(api_url,headers=headers)
        print("Printing the response of post %s",r.status_code)
        print("checking logs")
        print("checking logs status")
        if r.status_code == 200:
            self.make_get_api_call()

    def  make_get_api_call(self):
        bearer_token='icm-J0oPxzJ/ryOobrcy+ynQ6UyoHKnNfxo2PLJl9PeZvUg='
        headers = {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + bearer_token,
                    'Model': 'DeloitteTraining'
                    }
        api_url='https://spm.varicent.com/api/v1/auditlog'
        su = requests.get(api_url,headers=headers)
        logging.info("Printing the response the var %s",su.json())
        s1 = json.dumps(su.json())
        json_object = json.loads(s1)
        self.get_api_call(json_object)
            

    def format_schema(self,schema):
        formatted_schema = []
        for row in schema:
            formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
        return formatted_schema

    def get_api_call(self,json_object):

        project_id = 'us-gcp-ame-con-brdwu-sbx-1'
        dataset_id = 'VZPOCDS1'
        table_id = 'test_apicall'

        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = self.format_schema(self.table_schema)
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        
    

def main():
    api_call = APICall()
    api_call.extract_gcs_files()


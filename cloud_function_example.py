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

def transfer_file(event, context):

    file = event
    file_name = file['name']
    if file_name.split("/")[1] == 'Rejected':
        client = storage.Client()
        bucket = client.bucket("vz-poc-gcs1")
        blob = bucket.blob("VzDIPOC/Rejected/Vz_Test_Payee_To_Plan_rejected.csv")
        dest_file = '/tmp/tmprejected.csv'
        blob.download_to_filename(dest_file)
        line_count = 0
        print("coming into rejected file loop")
        with open(dest_file) as fh:
            print("coming into rejected file loop1")
            rd = csv.reader(fh, delimiter=',')
            print("coming into rejected file loop2")
            for row in rd:
                print("row")
                print(rd)
                line_count = line_count + 1
            
            print("printing line_count")
            print(line_count)
            if line_count == 1:
                blob.delete()

    if file_name.split("/")[1] == 'Varicent':
        client = storage.Client()
        bucket = client.bucket("vz-poc-gcs1")
        new_file_name= "VzDIPOC/Archive/Vz_Test_Payee_To_Plan" +  datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
        varicent_file_name = "Data/" + file_name.split("/")[2]
        print(varicent_file_name)
        gcs_file_name=file_name
        blob = bucket.blob(gcs_file_name)
        cnopts = pysftp.CnOpts()
        line_count = 0
        dest_good_file = '/tmp/tmpgood.csv'
        blob.download_to_filename(dest_good_file)
        cnopts.hostkeys = None

        with  pysftp.Connection(host='ftp.spm.ibmcloud.com', username='DeloitteTraining', password='Varicent*123',
                                                port=22,
                                                cnopts=cnopts) as sftp:
                        logging.info("Connection succesfully established ... ")
                        with sftp.open(varicent_file_name, 'w', 32768) as f:
                            blob.download_to_file(f)
                    
        #new_blob = bucket.copy_blob(
        #            blob, bucket, new_file_name)
        #print("coming to here in archive")
        #print(new_file_name)
        #blob.delete()
        if file_name.split("/")[2] == "STMAssignments_Components.txt":
            api_number='98'
        elif file_name.split("/")[2] == "STMAssignments_Segments.txt":
            api_number='99'
        else:
            api_number=''
        post_api_call(api_number)
    
def post_api_call(api_numner):

      #  from_date = str(dates[0])
      #  to_date = str(dates[1])
      bearer_token='icm-J0oPxzJ/ryOobrcy+ynQ6UyoHKnNfxo2PLJl9PeZvUg='
      headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + bearer_token,
                'Model': 'DeloitteTraining'
                }
      api_url='https://spm.varicent.com/api/v1/rpc/imports/' + api_numner + '/run'
      r = requests.post(api_url,headers=headers)
      print("Printing the response of post %s",r.status_code)
      print("checking logs")
      print("checking logs status")
      if r.status_code == 200:
        make_get_api_call()
       

table_schema = {
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


def  make_get_api_call():
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
    get_api_call(json_object)
           

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_api_call(json_object):

    project_id = 'us-gcp-ame-con-brdwu-sbx-1'
    dataset_id = 'VZPOCDS1'
    table_id = 'test_apicall'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, table, job_config = job_config)

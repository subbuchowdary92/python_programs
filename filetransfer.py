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
import sys


class transferfile(object):

        def __init__(self):
            self.environment=''
        
        def file_transfer(self,file_name):
            client = storage.Client()
            bucket = client.bucket("vz_gcomm_vdm_di_landing")
            input_file_name='Varicent/Readyforvaricent/' + file_name
            varicent_file_name = "Data/" + file_name
            print(varicent_file_name)
            gcs_file_name=input_file_name
            blob = bucket.blob(gcs_file_name)
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None

            with  pysftp.Connection(host='ftp.spm.ibmcloud.com', username='DeloitteTraining', password='Varicent*123',
                                                    port=22,
                                                    cnopts=cnopts) as sftp:
                            logging.info("Connection succesfully established ... ")
                            with sftp.open(varicent_file_name, 'w', 32768) as f:
                                blob.download_to_file(f)
                                print("file transferred successfully")


def main():
    try:

        if len(sys.argv) != 2:
            sys.exit("Not enough args")

       
        file_name = sys.argv[1]
      
        
        varobj = transferfile()
        varobj.file_transfer(file_name)


    except Exception as errormsg:
        logging.exception(print("Exception :",errormsg))
        raise sys.exit(1)

    return "Success"

if __name__ == '__main__':
    main()

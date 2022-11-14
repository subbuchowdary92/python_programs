import mysql.connector
from mysql.connector.constants import ClientFlag
from google.cloud import secretmanager
import os


class ExecuteCustomUtils:


    def connectivity(self):

        secretmanager_client = secretmanager.SecretManagerServiceClient()
        response1 = secretmanager_client.access_secret_version(
            name=f'projects/ms-cps-adp-dicom-etl-d/secrets/sa-dicomweb-key/versions/5')
        response2 = secretmanager_client.access_secret_version(
            name=f'projects/ms-cps-adp-dicom-etl-d/secrets/sa-dicomweb-key/versions/7')
        response3 = secretmanager_client.access_secret_version(
            name=f'projects/ms-cps-adp-dicom-etl-d/secrets/sa-dicomweb-key/versions/6')

        payload1 = response1.payload.data.decode("UTF-8")
        outF = open("/tmp/client-cert.pem", "w")
        outF.writelines(payload1)
        outF.close()

        payload2 = response2.payload.data.decode("UTF-8")
        outF = open("/tmp/client-key.pem", "w")
        outF.writelines(payload2)
        outF.close()

        payload3 = response3.payload.data.decode("UTF-8")
        outF = open("/tmp/server-ca.pem", "w")
        outF.writelines(payload3)
        outF.close()

        config = {
            'user': 'root',
            'password': 'root',
            'host': '10.2.240.39',
            'client_flags': [ClientFlag.SSL],
            'ssl_ca': '/tmp/server-ca.pem',
            'ssl_cert': '/tmp/client-cert.pem',
            'ssl_key': '/tmp/client-key.pem',
            'database': 'dataset_library'
        }
        connection = mysql.connector.connect(**config)
        return connection

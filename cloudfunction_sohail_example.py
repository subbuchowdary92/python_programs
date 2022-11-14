# Importing pandas, googleapiclient, datetime, logging, bigquery and os module
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
from execute_utils.utils import ExecuteCustomUtils
import google.cloud.logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
import os

def runner(event, context):

    utility = ExecuteCustomUtils()
    today = datetime.now()
    current_date_time = today.strftime("%Y-%m-%d %H:%M:%S")
    current_date = today.strftime("%Y-%m-%d")

    connection = utility.connectivity()
    cursor = connection.cursor()

    # Instantiate bq client
    bq_client = bigquery.Client()

    # Instantiates a client for logging
    client = google.cloud.logging.Client(project=os.environ.get('LOGGING_PROJECT'))
    # Retrieves a Cloud Logging handler based on the environment
    client.get_default_handler()
    client.setup_logging()

    # Fetch data from FILES and FILE_REQUESTS table for files approved to be loaded to bigquery
    dataset_data = pd.read_sql_query(
            "SELECT A.FILE_ID,A.FILE_NAME,B.REQUEST_ID,A.DATASET_NAME,A.FILE_LOCATION,A.FILE_TYPE, B.REQUESTED_BY FROM FILES A RIGHT JOIN  \
            FILE_REQUESTS B ON A.FILE_ID = B.FILE_ID WHERE B.REQUEST_TYPE = '6' AND B.REQUEST_ACTION_STATUS = 3 AND \
            A.FILE_CURRENT_INDICATOR = 1 AND A.FILE_UPLOAD_STATUS = 6 AND B.FILE_LOAD_STATUS = 0",
        connection)
    files = dataset_data.to_dict('records')

    dataflow_project = os.environ.get('DF_PROJECT')
    bq_project = os.environ.get('BQ_PROJECT')
    environment = {
        'temp_location': os.environ.get('DF_TEMP_LOCATION'),
        'serviceAccountEmail': os.environ.get('DF_CONTROLLER_SA')
    }
    for i in files:
        df_job_runtime_parameters = {}
        job_name = "AIDE" + "-" + i['FILE_NAME'] + current_date_time
        location_list = i['FILE_LOCATION'].split('/')
        table_name = location_list[2] + "_" + location_list[3]
        table_dataset_name = location_list[1].replace('-', '_').lower()
        file_path_list = location_list[1:]
        file_path = '/'.join([str(x) for x in file_path_list])
        df_job_runtime_parameters['bucket'] = location_list[0]
        df_job_runtime_parameters['file_path'] = file_path
        df_job_runtime_parameters['file_name'] = i['FILE_NAME']
        df_job_runtime_parameters['file_type'] = i['FILE_TYPE']
        df_job_runtime_parameters['source_file_path'] = "gs://" + i['FILE_LOCATION']
        df_job_runtime_parameters['table_name'] = bq_project + ":" + table_dataset_name + "." + table_name
        service = build('dataflow', 'v1b3', cache_discovery=False)
        if i['FILE_TYPE'] == 'csv':
            df_job_runtime_parameters['reject_location'] = os.environ.get('DF_REJECT_LOCATION') + i['FILE_NAME'] + ".reject"
            template = os.environ.get('DF_CSV_TEMPLATE')
        else:
            template = os.environ.get('DF_AVRO_TEMPLATE')

        # Create a dataset if not present
        try:
            bq_client.get_dataset(bq_project + "." + table_dataset_name)
        except NotFound:
            dataset = bigquery.Dataset(bq_project + "." + table_dataset_name)
            dataset.location = "US"
            dataset = bq_client.create_dataset(dataset)
        # Launch dataflow job

        launch_request = service.projects().locations().templates().launch(
            projectId=dataflow_project,
            gcsPath=template,
            location='us-central1',
            body={
                'jobName': job_name,
                'parameters': df_job_runtime_parameters,
                'environment': environment
            },
        )
        launch_response = launch_request.execute()
        logging.info(f"df_job_runtime_parameters---------{df_job_runtime_parameters}")
        logging.info(f"launch_response---------{launch_response}")

        update_status_query = f"UPDATE FILE_REQUESTS SET FILE_LOAD_STATUS = 5, UPDATE_DTM = '{current_date_time}' " \
                              f"WHERE REQUEST_ID = '{i['REQUEST_ID']}'"

        insert_status_query = f"INSERT INTO FILE_REQUESTS (REQUEST_ACTION_DATE, REQUEST_DATE, REQUEST_TYPE, FILE_ID, \
        REQUEST_ACTION_STATUS, CREATE_DTM, UPDATE_DTM, JOB_ID, REQUESTED_BY) VALUES ('{current_date}', '{current_date}', 3, {i['FILE_ID']},\
         5, '{current_date_time}', '{current_date_time}', '{launch_response['job']['id']}', '{i['REQUESTED_BY']}')"

        add_table_name_query = f"UPDATE FILES set FILE_TABLE_NAME = '{df_job_runtime_parameters['table_name']}' \
        where file_id = '{i['FILE_ID']}'"

        print(insert_status_query)
        cursor.execute(update_status_query)
        cursor.execute(insert_status_query)
        cursor.execute(add_table_name_query)
        connection.commit()
    connection.close()
    return "Successful"

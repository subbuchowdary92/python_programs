import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime
from datetime import date
from datetime import datetime
from apache_beam.io import WriteToText
from apache_beam.pvalue import TaggedOutput
from apache_beam import pvalue
from google.cloud import bigquery


class executesp(beam.DoFn):
    def process(self, element):
        query = 'CALL Admin.ltd_bq()'
        logging.info("query again here")
        logging.info(query)
        client = bigquery.Client(project='us-gcp-ame-con-brdwu-sbx-1')
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        dm_statement = query
        query_job = client.query(dm_statement,job_config=job_config)
       

def run(argv=None):
    global is_empty_check
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()
   
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='VZPOCDS1.onehr_payee_to_plan')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)


    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    query = "select * FROM `VZPOCDS1.onehr_payee_to_plan`"
    read_from_file = (p |  'Execute the Stored procedure' >> beam.ParDo(executesp()))
     
    

    
                                        
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

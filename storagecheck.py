import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime
from datetime import datetime, timedelta
from apache_beam.io import WriteToText
from apache_beam.pvalue import TaggedOutput
from apache_beam import pvalue
from google.cloud import bigquery
from collections import OrderedDict
import json

print(beam.io.gcp.gcsio.GcsIO().exists('gs://vz-poc-gcs1/VzDIPOC/Varicent/*Components.txt'))
   

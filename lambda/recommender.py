import json
import boto3
import pandas as pd
import os
import logging
from utils import utils

log = logging.getLogger()
log.setLevel(logging.INFO)

def handler(event, lambda_context):
    log.info("Survey processing request received.")
    request_body = event['Records'][0]
    log.info("Request parameters received as: {}".format(json.dumps(request_body)))
    #TODO: Process request parameters

    log.info("Fetching survey responses.")
    s3_client = boto3.client("s3")
    fp = utils.get_file(s3_client, os.environ.get("S3_BUCKET"), os.environ.get("SURVEY_OBJ"))
    log.info("Survey response path: {}".format(fp))

    df = pd.read_csv(fp)
    
    return

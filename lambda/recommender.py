import json
import boto3
import pandas as pd
import os
import logging
from utils import utils

RECS_DESTINATION_KEY = "processed_recommendations/recs.csv"

log = logging.getLogger()
log.setLevel(logging.INFO)


def handler(event, lambda_context):
    log.info("Survey processing request received.")
    request_body = event['Records'][0]
    log.info("Request parameters received as: {}".format(json.dumps(request_body)))
    #TODO: Process request parameters

    log.info("Fetching survey responses.")
    s3_client = boto3.client("s3")
    bucket = os.environ.get("S3_BUCKET")
    survey_obj = os.environ.get("SURVEY_OBJ")
    fp = utils.get_file(s3_client, bucket, survey_obj)
    log.info("Survey response path: {}".format(fp))

    df = pd.read_csv(fp)
    log.info("Success creating dataframe from csv.")
    recs = process_recommendations(df)
    log.info("Success calculating coefficients.")
    utils.write_df_to_s3(s3_client, recs, bucket, RECS_DESTINATION_KEY)
    log.info("Success writing recs to s3://{}/{}".format(bucket, RECS_DESTINATION_KEY))

    return


def process_recommendations(df):
    df = utils.data_clean(df)
    recs_json_arr = []
    for _, res in df[2:].iterrows():
        for _, can in df [2:].iterrows():
            dem_res = utils.demo_arr(res)
            dem_can = utils.demo_arr(can)
            int_res = utils.interests_arr(res)
            int_can = utils.interests_arr(can)
            recs_json_arr.append({
                "respondentId": res["ResponseId"],
                "candidateId": can["ResponseId"],
                "respondentName": res["Q1"],
                "candidateName": can["Q1"],
                "sim_coef": utils.sim_coef(dem_res, dem_can, int_res, int_can),
                "sim_diff_coef": utils.sim_diff_coef(dem_res, dem_can, int_res, int_can)
            })

    recs_df = pd.DataFrame(recs_json_arr)
    recs_df = recs_df[recs_df["respondentId"] != recs_df["candidateId"]]
    recs_df = recs_df.groupby(["respondentId"]).apply(lambda x: x.sort_values(["sim_diff_coef"], ascending = False)).reset_index(drop=True)
    return recs_df
    

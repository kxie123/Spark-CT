import json
import boto3
import pandas as pd
import os
import logging
from utils.ses_utils import send_emails
from utils import utils
from utils import ses_utils

QUEUE_NAME_PREFIX = "CtpsSparkRecommenderRequestQueue"
RECS_DESTINATION_KEY = "processed_recommendations/recs{}.csv"

log = logging.getLogger()
log.setLevel(logging.INFO)


def handler(event, lambda_context):
    log.info("Survey processing request received.")
    try:
        request = event['Records'][0]
        log.info("Request parameters received as: {}".format(request))
        req_att = request["messageAttributes"]
        log.info("Request attributes received as: {}".format(req_att))
        log.info("Fetching survey responses.")
        s3_client = boto3.client("s3")
        bucket = os.environ.get("S3_BUCKET")
        survey_obj = os.environ.get("SURVEY_OBJ")
        fp = utils.get_file(s3_client, bucket, survey_obj)
        log.info("Survey response path: {}".format(fp))

        df = pd.read_csv(fp)
        log.info("Success creating dataframe from csv.")
        if json.loads(req_att["split"]["stringValue"].lower()):
            rec_a, rec_b = process_recommendations(df, split=True)
            rec_key_a = RECS_DESTINATION_KEY.format("_a")
            rec_key_b = RECS_DESTINATION_KEY.format("_b")
            utils.write_df_to_s3(s3_client, rec_a, bucket, rec_key_a)
            utils.write_df_to_s3(s3_client, rec_a, bucket, rec_key_b)
            log.info("Success writing recs to s3://{}/{}".format(bucket, rec_key_a))
            log.info("Success writing recs to s3://{}/{}".format(bucket, rec_key_b))

        recs = process_recommendations(df)
        log.info("Success calculating coefficients.")
        rec_overall_key = RECS_DESTINATION_KEY.format("_overall")
        utils.write_df_to_s3(s3_client, recs, bucket, rec_overall_key)
        log.info("Success writing recs to s3://{}/{}".format(bucket, rec_overall_key))
    except Exception as e:
        log.exception(e)
    finally:
        try:
            sqs_client = boto3.client("sqs")
            log.info("Attempting to remove request from queue...")
            utils.delete_request(sqs_client, QUEUE_NAME_PREFIX, request)
            log.info("Request removed from queue.")
        except Exception as e:
            log.exception(e)

    #TODO: Send emails through SES
    #log.info("Sending survey link and emails...")
    #ses_utils.send_emails()

    return


def process_recommendations(df, split=False):
    df = utils.data_clean(df)
    if split:
        group_a = df[2:].sample(frac = 0.5)
        group_b = df[2:].drop(group_a.index)
        return get_recommendations_df(group_a), get_recommendations_df(group_b)
    return get_recommendations_df(df[2:])


def get_recommendations_df(df):
    recs_json_arr = []
    for _, res in df.iterrows():
        for _, can in df.iterrows():
            dem_res = utils.demo_arr(res)
            dem_can = utils.demo_arr(can)
            int_res = utils.interests_arr(res)
            int_can = utils.interests_arr(can)
            recs_json_arr.append({
                "respondentId": res["ResponseId"],
                "respondentName": res["Q1"],
                "respondentEmail": res["Q3"],
                "candidateId": can["ResponseId"],
                "candidateName": can["Q1"],
                "candidateEmail": can["Q3"],
                "sim_coef": utils.sim_coef(dem_res, dem_can, int_res, int_can),
                "sim_diff_coef": utils.sim_diff_coef(dem_res, dem_can, int_res, int_can)
            })

    recs_df = pd.DataFrame(recs_json_arr)
    recs_df = recs_df[recs_df["respondentId"] != recs_df["candidateId"]]
    recs_df = recs_df.groupby(["respondentId"]).apply(lambda x: x.sort_values(["sim_diff_coef"], ascending = False)).reset_index(drop=True)
    return recs_df
    
import os
import uuid
import pandas as pd

ordinal_income_map = {
  #TODO: Define mapping
}

ordinal_political_map = {
    "Liberal": 1,
    "Progressive": 2,
    "Apolotical": 3,
    "I do not wish to disclose": 3,
    "Moderate": 4,
    "Conservative": 5
}

ordinal_grade_map = {
    "9": 1,
    "10": 2,
    "11": 3,
    "12": 4
}

def get_file(s3_client, bucket, key):
    tmp = "/tmp/{}".format(uuid.uuid4())
    s3_client.download_file(bucket, key, tmp)
    return tmp

def filter_no_interest(df):
    return df[df.Q14 != "No"]


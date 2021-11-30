import os
import uuid
import pandas as pd
from io import StringIO

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


def delete_request(sqs_client, queue_name, request):
    queues = sqs_client.list_queues(QueueNamePrefix=queue_name)
    queue_url = queues["QueueUrls"][0]
    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=request["receiptHandle"])


def get_file(s3_client, bucket, key):
    """Downloads file from s3 to local tmp dir.
    """
    tmp = "/tmp/{}".format(uuid.uuid4())
    s3_client.download_file(bucket, key, tmp)
    return tmp


def write_df_to_s3(s3_client, df, bucket, key):
    """Write a df to s3.
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=key)


def data_clean(df):
    """Fill na values in dataset.
    """
    df["Q7"] = df["Q7"].fillna("I do not wish to disclose")
    df["Q7"] = df["Q8"].fillna("I do not wish to disclose")
    return df


def jaccard_coef(a, b):
    """Calculates the jaccard similarity score between sets a and b.
    """
    a = set(a)
    b = set(b)
    intersection = len(list(a.intersection(b)))
    union = (len(a) + len(b)) - intersection
    return float(intersection) / union


def jaccard_dist(a,  b):
    """Calculates jaccard distance from similarity.
    """
    return 1 - jaccard_coef(a, b)


def interests_arr(row):
    """Create arr of all interest inputs.
    """
    arr = []
    for q in ["Q10", "Q11", "Q13", "Q15"]:
        arr += row[q].split(",")
    return arr


def demo_arr(row):
    """Create arr of all demographic inputs.
    """
    return [row["Q2"], row["Q4"], row["Q6"], 
        row["Q7"], row["Q8"]
    ]


def sim_coef(dem_a, dem_b, int_a, int_b):
    """Calculate similarity coefficient.
    """
    return jaccard_coef(dem_a+int_a, dem_b+int_b)


def sim_diff_coef(dem_a, dem_b, int_a, int_b):
    """
    Given demographics and interests of two people,
    calculate jaccard distance of demographics and 
    average that with the jaccard coefficient of
    interests.
    """
    dem_jac_dist = jaccard_dist(dem_a, dem_b)
    int_jac_dist = jaccard_coef(int_a, int_b)
    return (dem_jac_dist + int_jac_dist) / 2


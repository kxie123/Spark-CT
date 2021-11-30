import boto3
import logging
import argparse
import sys

REGION_NAME = "us-east-1"
QUEUE_NAME_PREFIX = "CtpsSparkRecommenderRequestQueue"
MESSAGE_ATTRIBUTES = dict.fromkeys(["split"])

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--split', dest='split', action='store_true')
    parser.add_argument('--no-split', dest='split', action='store_false')
    parser.set_defaults(split=True)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    sqs_client = boto3.client("sqs")
    queues = sqs_client.list_queues(QueueNamePrefix=QUEUE_NAME_PREFIX)
    queue_url = queues["QueueUrls"][0]
    MESSAGE_ATTRIBUTES["split"] = {
        "StringValue": str(args.split), "DataType": "String"
    }
    sqs_client.send_message(QueueUrl=queue_url, MessageBody="Recommendation request", MessageAttributes=MESSAGE_ATTRIBUTES)


if __name__ == "__main__":
    main()

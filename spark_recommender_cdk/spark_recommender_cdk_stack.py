from aws_cdk import (
    core as cdk,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as les,
    aws_iam as iam
)

BUCKET_NAME = "ctps-spark-survey-responses"
SURVEY_OBJ = "responses/ctsp-spark-survey-responses.csv"
#Source: https://github.com/keithrozario/Klayers/tree/master/deployments/python3.8/arns
AWS_PANDAS_38_LAYER_ARN = "arn:aws:lambda:us-east-1:770693421928:layer:Klayers-python38-pandas:43"

class SparkRecommenderCdkStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #Create survey response bucket with KMS encryption
        survey_bucket = s3.Bucket(self, "SurveyResponseBucket",
            bucket_name=BUCKET_NAME,
            versioned=True,
            encryption=s3.BucketEncryption.KMS_MANAGED,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
        #Create sqs queue
        request_queue = sqs.Queue(self, "RequestQueue",
            queue_name="CtpsSparkRecommenderRequestQueue",
        )
        #create recommender lambda construct
        recommender_lambda = _lambda.Function(
            self, "CtpsSparkRecommenderLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.minutes(15),
            code=_lambda.Code.from_asset("lambda"),
            handler="recommender.handler",
            environment={
                "S3_BUCKET": BUCKET_NAME,
                "SURVEY_OBJ": SURVEY_OBJ
            },
            layers=[
                _lambda.LayerVersion.from_layer_version_arn(self, "AWS_PANDAS_LAYER", AWS_PANDAS_38_LAYER_ARN),
            ]
        )
        #Trigger lambda execution with SQS messsage
        recommender_lambda.add_event_source(
            les.SqsEventSource(request_queue)
        ) 
        #Allow lambda to delete bad request
        recommender_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "sqs:ListQueues",
                    "sqs:DeleteMessage"
                ],
                resources=["*"]
            )
        )
        recommender_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail"
                ],
                resources=["*"]
            )
        )
        #grant permission for lambda to read write to survey bucket
        survey_bucket.grant_read_write(recommender_lambda)

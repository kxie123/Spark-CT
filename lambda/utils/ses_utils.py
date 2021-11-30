import boto3
import logging
from botocore.exceptions import ClientError

SENDER = ""

RECIPIENT = ""

# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
AWS_REGION = "us-east-1"

# The subject line for the email.
SUBJECT = "Recommendation Test"

# The email body for recipients with non-HTML email clients.
BODY_TEXT = ("Amazon SES Test (Python)\r\n"
             "This email was sent with Amazon SES using the "
             "AWS SDK for Python (Boto)."
            )
            
# The HTML body of the email.
BODY_HTML = """<html>
<head></head>
<body>
  <h1>Hello, student!</h1>
  <p>Thank you for participating in Spark Recommendation Matching! 
     We have matched you with three students who we think you'd be interested in matching with!
     Please listen to the audio clips and fill out the following survey:
    <a href='https://cornell.ca1.qualtrics.com/jfe/form/SV_0vQSvSzWKOJHeFU'>Recommendation survey</a></a>.</p>
</body>
</html>
            """            

# The character encoding for the email.
CHARSET = "UTF-8"

log = logging.getLogger()
log.setLevel(logging.INFO)


def send_emails(mail_dict):
    #TODO: Send email audio clips to mail list.
    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                    SENDER
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        log.exception(e.response['Error']['Message'])
    else:
        log.info("Email sent! Message ID:"),
        log.info(response['MessageId'])

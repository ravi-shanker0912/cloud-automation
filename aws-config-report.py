import boto3
import csv
import json
import os
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from botocore.exceptions import ClientError

def create_report(aggregator_name, today):
    
    client = boto3.client('config')
    response = client.select_aggregate_resource_config(
        Expression=f"SELECT * WHERE configurationItemCaptureTime LIKE '{today}%'",
        ConfigurationAggregatorName=aggregator_name
    )
    changed_resources = response["Results"]
    while "NextToken" in response:
        nt = response["NextToken"]
        response = client.select_aggregate_resource_config(
            Expression=f"SELECT * WHERE configurationItemCaptureTime LIKE '{today}%'",
            ConfigurationAggregatorName=aggregator_name,
            NextToken=nt
        )
        changed_resources.extend(response["Results"])
    
    json_list = [json.loads(line) for line in changed_resources]
    filename = f"/tmp/config_report_{today}.csv"
    
    for resource in json_list:
        AWS_REGION = resource['awsRegion']
        RESOURCE_ID = resource['resourceId']
        RESOURCE_TYPE = resource['resourceType']
        resource['Link'] = get_link(AWS_REGION, RESOURCE_ID, RESOURCE_TYPE)      
    all_fields = set()
    for item in json_list:
        all_fields.update(item.keys())
    # Save the report file
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(all_fields))
        writer.writeheader()
        writer.writerows(json_list)
    print("Report generated "+ filename)
    return filename
    

def get_link(AWS_REGION, RESOURCE_ID, RESOURCE_TYPE):
    url = f'https://{AWS_REGION}.console.aws.amazon.com/config/home?region={AWS_REGION}#/resources/timeline?resourceId={RESOURCE_ID}&resourceType={RESOURCE_TYPE}'
    return url

def upload_to_s3(filename, bucket_name, prefix):
    s3_client = boto3.client('s3')
    today = datetime.datetime.now()
    year = today.strftime('%Y')
    month = today.strftime('%m')
    day = today.strftime('%d')
    key = f"{prefix}/{year}/{month}/{day}/{os.path.basename(filename)}"
    s3_client.upload_file(filename, bucket_name, key)
    return key

def send_email(today, SENDER, RECIPIENT, filename, s3_bucket, s3_prefix):
    
    SUBJECT = f"AWS Config changes report for {today}"
    ATTACHMENT = filename
    BODY_TEXT = "Hello,\r\nPlease see the attached file which includes the changes made during the last day."
    ses = boto3.client('ses')

    BODY_HTML = f"""\
    <html>
    <head></head>
    <body>
    <p>Hello All,</p>
    <p>PFA configuration changes report for today. This report contains all the configuration changes related to all the AWS services across all the AWS accounts.</p>
    <p>Regards,<br/>Ravi Shanker</p>
    </body>
    </html>
    """
    CHARSET = "utf-8"
    msg = MIMEMultipart('mixed')
    msg['Subject'] = SUBJECT
    msg['From'] = SENDER
    msg['To'] = RECIPIENT
    msg_body = MIMEMultipart('alternative')
    textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    htmlpart = MIMEText(BODY_HTML.encode(CHARSET), 'html', CHARSET)
    msg_body.attach(textpart)
    msg_body.attach(htmlpart)
    att = MIMEApplication(open(ATTACHMENT, 'rb').read())
    att.add_header('Content-Disposition', 'attachment',
                   filename=os.path.basename(ATTACHMENT))
    msg.attach(msg_body)
    msg.attach(att)
    try:
        response = ses.send_raw_email(
            Source=SENDER,
            Destinations=[RECIPIENT],
            RawMessage={'Data': msg.as_string()}
        )
        print("Email sent! Message ID:")
        print(response['MessageId'])
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:")
        print(response['MessageId'])

def lambda_handler(event, context):
    filename = create_report(aggregator_name, today)
    s3_bucket = 'bucket-name'
    s3_prefix = 'daily-config-report'
    key = upload_to_s3(filename, s3_bucket, s3_prefix)
    send_email(today, SENDER, RECIPIENT, filename, s3_bucket, s3_prefix)
    aggregator_name = 'aggregator-name'
    SENDER = 'Email of Sender'
    RECIPIENT = 'Email id recipient'
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    

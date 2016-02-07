
import boto3 
from util.config import *

def send_email_alert(sender, subject, body, to, format="text"):
    """
    Sends email alert.
    You may need to verify the sender address by using botoconn.verify_email_address('led-alerts@umich.edu') and clicking the link in the verification email sent to this address.
    """
    
    aws_access_key = get_properties().get('access_id', None)
    aws_secret_key = get_properties().get('secret_key', None)

    client = boto3.client('ses', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    alert_destination = {
        'ToAddresses': [to,],
        'CcAddresses': [None,]
        }

    if format=="html":
        alert_contents = {
        'Subject' : {'Data' : subject},
        'Body' : {'Html' : { 'Data' : body}}
        }
    
    else:
        alert_contents = {
        'Subject' : {'Data' : subject},
        'Body' : {'Text' : { 'Data' : body}}
        }

    client.send_email(Source = sender, Destination = alert_destination, Message = alert_contents)



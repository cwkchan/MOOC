
import boto3 
#note: boto3 is the newest version of boto and is distinct from previous boto module.





def send_email_alert(sender, subject, body, to, aws_access_key_id, aws_secret_access_key, format="text"):
    """
    If you're using this with new credentials or change the sender, you may need to verify the sender address.

    To verify a new email address, use the following command (replace with desired sender address):

    botoconn.verify_email_address('led-alerts@umich.edu')

    You will need to click the link in the verification email sent to this address to be able to send email from it.
    """
    

    client = boto3.client('ses', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    alert_destination = {

    'ToAddresses': [
            to,
        ],
        'CcAddresses': [
            'joshua.patrick.gardner@gmail.com',
        ]
    }

    #import ipdb; ipdb.set_trace()

    if format=="html":
        alert_contents = {

        'Subject' : {'Data' : subject},

        'Body' : {'Html' : { 'Data' : body}
             }
        }
    
    else:
        alert_contents = {

        'Subject' : {'Data' : subject},

        'Body' : {'Text' : { 'Data' : body}
             }
        }

    client.send_email(Source = sender, Destination = alert_destination, Message = alert_contents)



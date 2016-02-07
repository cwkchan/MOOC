
import boto3 
#note: boto3 is the newest version of boto and is distinct from previous boto module.



#note: update access key and secret access key
aws_access_key_id="AKIAJ5AY3SPCD5PF7UKQ"
aws_secret_access_key="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"


def send_email_alert(sender, subject, body, to, format="text"):
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


def main():
	
    send_email_alert(sender = "led-alerts@umich.edu", subject = 'LED Lab AWS UpdateTEST', body = 'Messsage Body: This is a test of the Boto3 email update alert.', to = 'jpgard@umich.edu')
    

if __name__ == '__main__':
	print "Sending Message..."
	main()
	print "Complete"


import os
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

########################################################

def send_email(sender_address, sender_password, recipients_addresses, subject, message_field, images_list=None, dataframes=None):

    '''
    sender_address: Gmail address the email is coming from
    sender_password: Password for the sender_address account
    recipients_addresses: A list of addresses for the recipients. Must be a list
    subject: Subject field of the email
    message_field: Body of the email
    images_list: File paths for images to attatch. Do not fill if you are not sending an image.
    dataframes: Pass in a list of html dataframes filepaths

    Takes in the above variables and sends an email from the sender address to the recipient(s)
    address(es) with the declared subject and body fields.
    '''

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender_address
    for recipient in recipients_addresses:

        msg['To'] = recipient

    body = MIMEText(message_field)
    msg.attach(body)

    ########## Attatching Images ##########

    if images_list != None:

        for image_path in images_list:

            img_data = open(image_path, 'rb').read()
            image = MIMEImage(img_data, name=os.path.basename(image_path))
            msg.attach(image)

    else:

        pass

    if dataframes != None:

        for frame in dataframes:

            filename = frame
            f = open(filename)
            attachment = MIMEText(f.read(),'html')
            msg.attach(attachment)

    else:

        pass

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(sender_address, sender_password)

    s.sendmail(sender_address, recipients_addresses, msg.as_string())

    s.quit()

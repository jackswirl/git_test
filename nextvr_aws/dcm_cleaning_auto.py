import pandas as pd
import MySQLdb
from sqlalchemy import create_engine
import imaplib
import email
from datetime import date, timedelta
import datetime
import numpy as np
import StringIO
import sys
import warnings
if not sys.warnoptions:
    warnings.simplefilter("ignore")

today = datetime.date.today().strftime("%Y%m%d")
dcm = pd.DataFrame()

mail = imaplib.IMAP4_SSL('outlook.office365.com')
mail.login('caitlin.mowdy@mcgarrybowen.com', 'Yodayo2!')
result, mailboxes = mail.list()
mail.select("INBOX")

result, data = mail.search(None,'(SUBJECT "Next VR 2017 DCM Delivery_no floodlights")')
email_list = data[0].split(" ")
for i in email_list:
    result, data = mail.fetch(i, "(RFC822)")
    raw_email = data[0][1]
    raw_email_string = raw_email.decode('utf-8')
    email_message = email.message_from_string(raw_email_string)

    sio = StringIO.StringIO()
    for part in email_message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        filename = part.get_filename()
        if today in filename:
            sio = StringIO.StringIO()
            data = part.get_payload(decode=True)
            sio.write(data)
            sio.seek(0)
            dcm = dcm.append(pd.read_csv(sio, header = 8, skipfooter=1))
            sio.close()

print "Pulled DCM Data For", dcm['Date'].unique()
dcm['Date'] = pd.to_datetime(dcm['Date'])
dcm['Date'] = dcm['Date'].dt.date

dcm['creative_concept'] = ['Tracking' if 'tracking' in i.lower() else i.split("_")[0] for i in dcm['Creative']]
dcm['cta'] = [np.NaN if 'tracking' in i.lower() else i.split("_")[1] for i in dcm['Creative']]
dcm['size'] = ['1x1' if 'tracking' in i.lower() else i.split("_")[2] for i in dcm['Creative']]
dcm['language'] = [np.NaN if 'tracking' in i.lower() else i.split("_")[3] for i in dcm['Creative']]
dcm['creative_type'] = ['Tracking' if 'tracking' in i.lower() else i.split("_")[4] for i in dcm['Creative']]

dcm = dcm[['Date','Creative ID','Creative','Impressions','Clicks','creative_concept','cta','size','language','creative_type']]
dcm.rename(columns={'Creative ID':'creative_id'},inplace=True)
print "Cleaned DCM"
engine = create_engine('mysql://jack:jack1620@swirl-data-cluster-1.cluster-cottqizfonjk.us-west-2.rds.amazonaws.com:3306/nextvr')
dcm.to_sql(name='dcm_aws_test',con=engine,if_exists='append',index=False, chunksize=1000)
print "Pushed DCM to SQL"

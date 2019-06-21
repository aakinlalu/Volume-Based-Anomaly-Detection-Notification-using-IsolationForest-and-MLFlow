from datetime import datetime, timedelta
import smtplib
import email
from email import encoders
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email import charset

import re

class Notification:
    def __init__(self, host, from_addr, to_addr, table, to_addr_cc=None, to_addr_bcc=None):
        self.host=host
        self.author=from_addr
        self.recipients=to_addr
        self.table = table
        self.cc_recipients=to_addr_cc
        self.bcc_recipients=to_addr_bcc
    
    def yestersday(self):
        yestersday = datetime.now() - timedelta(days=2)
        return datetime.strftime(yestersday, format='%B %d, %Y')


    def send_email(self, subject):
        
        regex = re.compile('\w+')
        name = regex.search(self.recipients).group()
        
        date = self.yestersday()
        
        message ="""
                 Hi <b>{}</b>, <br><br>
                   No single data delivered to <b>Redshift {}</b> table  on <b>{}</b>. <br>
                   If your dashboards or reporting views depend on this table, please notify your stakeholders.
                   <br>
                   <br>
                  <b>Note</b>: DevOps are working to fix the problem. For further info: Contact: XXXXXXXXXX). 
                   <br>
                   <br>
                  {}<br>
                  {}
                  """
        

        body = message.format(name, self.table, date, "Regards,","DI Anomaly System")
        
        msg = email.message.Message()

        #msg = MIMEText(body)
        msg.set_unixfrom(self.author)
        msg['To'] = self.recipients
        msg['Cc'] = self.cc_recipients
        msg['Bcc'] = self.bcc_recipients
        msg['From'] = self.author
        msg['Subject'] = subject
        msg.add_header('Content-Type', 'text/html')
        msg.set_payload(body)

        server = smtplib.SMTP(self.host)

        try:
            server.sendmail(self.author, self.recipients, msg.as_string())
        finally:
            server.quit()
    
    
    def send_email_2(self, subject, savepath):
        
        regex = re.compile('\w+')
        name = regex.search(self.recipients).group()
        
        date = self.yestersday()
        
        message = """
           <html>
           <body>
           Hi <b>{}</b>, <br><br>
             Anomaly has been detected in <b>Redshift {}</b> table  on <b>{}</b>. <br><br>
             Someone should check and pass the information to DevOps. Please refer to confluence page link below
             for <b>interpretation of the chart</b>.
             <br>
             <br>
             <img src="cid:{}" alt="Decision Score" width="400" height="300">
             <hr>
             <br>
             <b>Reference Information</b>: <XXXXXXXX>Anomaly Documentation.</a>
            <br>
            <b>For Further info: Contact</b>: XXXXX (XXXXXXX).
            <br>
            <br>
            {}<br>
            {}
            </body>
            </html>
           """
        
        body = message.format(name, self.table, date, savepath, "Regards,","DI Anomaly System")
        
        #msg = email.message.Message()
        msg = MIMEMultipart("alternative")
        msg.set_unixfrom(self.author)
        msg['To'] = self.recipients
        msg['Cc'] = self.cc_recipients
        msg['Bcc'] = self.bcc_recipients
        msg['From'] = self.author
        msg['Subject'] = subject
        
        part = MIMEText(body, 'html')
        msg.attach(part)
        
        #msg.add_header('Content-ID', <savepath>)
        
        with open(savepath, 'rb') as fp:
            image = MIMEImage(fp.read())
            
        image.add_header('Content-ID', '<'+savepath+'>')
        image.add_header("Content-Disposition", "in-line", filename=savepath)
        image.add_header('X-Attachment-Id', savepath) 
        
        msg.attach(image)
        
        #msg.set_payload(body)

        server = smtplib.SMTP(self.host)

        try:
            server.sendmail(self.author, self.recipients, msg.as_string())
        finally:
            server.quit()


import smtplib
from email.mime.text import MIMEText

def send_notification(subject, message):
    msg = MIMEText(message)
    msg["Subject"] = subject
    msg["From"] = "your_email@example.com"
    msg["To"] = "recipient_email@example.com"

    with smtplib.SMTP("smtp.example.com", 587) as server:
        server.login("your_email@example.com", "password")
        server.sendmail("your_email@example.com", ["recipient_email@example.com"], msg.as_string())

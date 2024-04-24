import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import ssl
import os


def send_email(from_email: str, to_email: str, subject: str, msg: str) -> None:
    """
    # Send email using smtp.gmail.com, password must be stored in env variable gmail_pw

    Parameters
    ----------
    from_email : str
        The email address from which the email will originate
    to_email : str
        The email address recipient
    subject : str
        The subject of the email
    msg : str
        The body of the email

    Returns
    -------
    No return

    """
    mimemsg = MIMEMultipart("alternative")
    mimemsg.set_charset("utf8")
    mimemsg["FROM"] = from_email
    mimemsg["To"] = to_email
    mimemsg["Subject"] = Header(subject, "utf-8")
    while msg.find("  ") > 0:
        msg = msg.replace("  ", " \u2800")
    msg = msg.replace("\n", "<br>")
    mimemsg.attach(MIMEText(msg.encode("utf-8"), "html", "UTF-8"))
    with smtplib.SMTP_SSL(
        "smtp.gmail.com", 465, context=ssl.create_default_context()
    ) as server:
        server.login(from_email, os.environ["gmail_pw"])
        server.sendmail(from_email, to_email, mimemsg.as_string())

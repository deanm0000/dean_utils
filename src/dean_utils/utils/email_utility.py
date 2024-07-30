import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import ssl
import os
from azure.communication.email import EmailClient


class MissingEnvVars(Exception):
    pass


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


try:
    email_client = EmailClient.from_connection_string(os.environ["azuremail"])
except:  # noqa: E722
    email_client = None


def az_send(
    subject: str | None = None,
    msg: str | None = None,
    html: str | None = None,
    from_email: str | None = None,
    to_email: str | None = None,
) -> None:
    if email_client is None:
        raise MissingEnvVars("missing azuremail var")
    if os.environ["error_email"] is not None and to_email is None:
        to_email = os.environ["error_email"]
    if os.environ["from_email"] is not None and from_email is None:
        from_email = os.environ["from_email"]
    content = {}
    if subject is not None:
        content["subject"] = subject
    if msg is not None:
        content["plainText"] = msg
    if html is not None:
        content["html"] = html
    email_client.begin_send(
        dict(
            senderAddress=from_email,
            recipients=dict(to=[{"address": to_email}]),
            content=content,
        )
    )

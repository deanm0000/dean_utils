from __future__ import annotations

import os

from azure.communication.email import EmailClient


class MissingEnvVars(Exception):
    pass


def az_send(
    subject: str | None = None,
    *,
    email_client_conn_str: str | None = None,
    msg: str | None = None,
    html: str | None = None,
    from_email: str | None = None,
    to_email: str | None = None,
) -> None:
    to_email = to_email or os.environ.get("error_email")
    if to_email is None:
        msg = "to_email is missing"
        raise KeyError(msg)
    from_email = from_email or os.environ.get("from_email")
    if from_email is None:
        msg = "from_email is missing"
        raise KeyError(msg)
    content = {}
    if subject is not None:
        content["subject"] = subject
    if msg is not None:
        content["plainText"] = msg
    if html is not None:
        content["html"] = html

    conn_str = email_client_conn_str or os.environ.get("azuremail")
    if conn_str is None:
        msg = "conn_str is missing"
        raise KeyError(msg)
    email_client = EmailClient.from_connection_string(conn_str)

    email_client.begin_send(
        {
            "senderAddress": from_email,
            "recipients": {"to": [{"address": to_email}]},
            "content": content,
        }
    )

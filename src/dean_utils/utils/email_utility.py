from __future__ import annotations

import os

from azure.communication.email import EmailClient


class MissingEnvVars(Exception):
    pass


try:
    email_client = EmailClient.from_connection_string(os.environ["azuremail"])
except:  # noqa: E722
    email_client = None


def az_send(
    subject: str | None = None,
    *,
    msg: str | None = None,
    html: str | None = None,
    from_email: str | None = None,
    to_email: str | None = None,
) -> None:
    if email_client is None:
        msg = "missing azuremail var"
        raise MissingEnvVars(msg)
    if os.environ.get("error_email") is not None and to_email is None:
        to_email = os.environ["error_email"]
    if os.environ.get("from_email") is not None and from_email is None:
        from_email = os.environ["from_email"]
    content = {}
    if subject is not None:
        content["subject"] = subject
    if msg is not None:
        content["plainText"] = msg
    if html is not None:
        content["html"] = html
    email_client.begin_send(
        {
            "senderAddress": from_email,
            "recipients": {"to": [{"address": to_email}]},
            "content": content,
        }
    )

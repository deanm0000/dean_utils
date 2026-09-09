from __future__ import annotations

import os
from pathlib import Path

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
    """
    Send an email via Azure Communication Services.

    Parameters
    ----------
    subject : str | None, default=None
        Email subject line. Omitted from the message if ``None``.
    email_client_conn_str : str | None, default=None
        Azure Communication Services connection string. Falls back to the
        ``azuremail`` environment variable.
    msg : str | None, default=None
        Plain-text body. Omitted from the message if ``None``.
    html : str | None, default=None
        HTML body. Omitted from the message if ``None``.
    from_email : str | None, default=None
        Sender address. Falls back to the ``from_email`` environment variable.
    to_email : str | None, default=None
        Recipient address. Falls back to the ``error_email`` environment
        variable.

    Raises
    ------
    MissingEnvVars
        If ``to_email``, ``from_email``, or the connection string is not
        supplied and no corresponding environment variable is set.

    """
    to_email = to_email or os.environ.get("error_email")
    if to_email is None:
        msg = "to_email is missing"
        raise MissingEnvVars(msg)
    from_email = from_email or os.environ.get("from_email")
    if from_email is None:
        msg = "from_email is missing"
        raise MissingEnvVars(msg)
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
        raise MissingEnvVars(msg)
    email_client = EmailClient.from_connection_string(conn_str)

    email_client.begin_send(
        {
            "senderAddress": from_email,
            "recipients": {"to": [{"address": to_email}]},
            "content": content,
        }
    )


def error_email(
    func,
    *,
    subject: str | None = None,
    email_client_conn_str: str | None = None,
    from_email: str | None = None,
    to_email: str | None = None,
    attempts: int = 1,
):
    """Wrapper to send an email if the decorated function raises an exception."""
    subject = subject or str(Path.cwd())

    def wrapper(*args, **kwargs):
        errors = []
        for _ in range(attempts):
            try:
                return func(*args, **kwargs)
            except BaseException as err:
                import inspect
                from traceback import format_exception

                filt_stack = "\n".join(
                    [
                        str(x)
                        for x in inspect.stack()[1:]
                        if "site-packages" not in x.filename
                    ]
                )
                errors.append("\n".join(["\n".join(format_exception(err)), filt_stack]))

        az_send(
            subject=subject,
            msg="\n".join(errors),
            email_client_conn_str=email_client_conn_str,
            from_email=from_email,
            to_email=to_email,
        )

    return wrapper

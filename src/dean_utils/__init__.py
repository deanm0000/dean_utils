__all__ = [
    "async_abfs",
    "clear_messages",
    "peek_messages",
    "get_queue_properties",
    "send_message",
    "update_queue",
    "delete_message",
    "send_email",
    "az_send",
    "pl_scan_hive",
    "pl_scan_pq",
    "pl_write_pq",
    "pl_write_delta_append",
    "global_async_client",
]
from dean_utils.utils.az_utils import (
    async_abfs,
    peek_messages,
    get_queue_properties,
    send_message,
    update_queue,
    delete_message,
    clear_messages,
)
from dean_utils.utils.email_utility import send_email, az_send
from dean_utils.polars_extras import (
    pl_scan_hive,
    pl_scan_pq,
    pl_write_pq,
    pl_write_delta_append,
)
from dean_utils.utils.httpx import global_async_client
from typing import cast, Iterable


def error_email(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            import os
            import inspect
            from traceback import format_exception

            email_body = (
                "\n".join(cast(Iterable[str], inspect.stack()))
                + "\n\n"
                + "\n".join(format_exception(err))
            )
            az_send(
                os.getcwd(),
                email_body,
            )

    return wrapper

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
import contextlib
from collections.abc import Iterable
from typing import cast

from dean_utils.polars_extras import (
    pl_scan_hive,
    pl_scan_pq,
    pl_write_pq,
)

with contextlib.suppress(ImportError):
    from dean_utils.polars_extras import pl_write_delta_append

from dean_utils.utils.az_utils import (
    async_abfs,
    clear_messages,
    delete_message,
    get_queue_properties,
    peek_messages,
    send_message,
    update_queue,
)
from dean_utils.utils.email_utility import az_send, send_email
from dean_utils.utils.httpx import global_async_client


def error_email(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            import inspect
            import os
            from traceback import format_exception

            email_body = (
                "\n".join(cast(Iterable[str], inspect.stack()))
                + "\n\n"
                + "\n".join(format_exception(err))
            )
            az_send(
                os.getcwd(),  # noqa: PTH109
                email_body,
            )

    return wrapper

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


def error_email(func, attempts=1):
    def wrapper(*args, **kwargs):
        subject = None
        errors = []
        for _ in range(attempts):
            try:
                return func(*args, **kwargs)
            except Exception as err:
                import inspect
                from pathlib import Path
                from traceback import format_exception

                if subject is None:
                    subject = Path.cwd()
                errors.append(
                    "\n".join(cast(Iterable[str], inspect.stack()))
                    + "\n\n"
                    + "\n".join(format_exception(err))
                )
        if subject is not None:
            az_send(
                str(subject),
                "\n".join(errors),
            )

    return wrapper

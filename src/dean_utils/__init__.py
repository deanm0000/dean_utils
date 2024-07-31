__all__ = [
    "async_abfs",
    "cos_query_all",
    "send_message",
    "update_queue",
    "delete_message",
    "send_email",
    "az_send",
    "def_cos",
    "pl_scan_hive",
    "pl_scan_pq",
    "pl_write_pq",
]
from dean_utils.utils.az_utils import (
    async_abfs,
    cos_query_all,
    send_message,
    update_queue,
    delete_message,
    def_cos,
)
from dean_utils.utils.email_utility import send_email, az_send
from dean_utils.polars_extras import pl_scan_hive, pl_scan_pq, pl_write_pq


def error_email(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            import os
            import inspect
            from traceback import format_exception

            az_send(
                os.getcwd(),
                "\n".join(inspect.stack()) + "\n\n" + "\n".join(format_exception(err)),
            )

    return wrapper

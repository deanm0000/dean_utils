from __future__ import annotations

from typing import Literal, TypeAlias, cast

__all__ = [
    "async_abfs",
    "az_send",
    "clear_messages",
    "delete_message",
    "get_queue_properties",
    "global_async_client",
    "peek_messages",
    "pl_scan_hive",
    "pl_scan_pq",
    "pl_write_delta_append",
    "pl_write_pq",
    "send_message",
    "update_queue",
]
import contextlib
import os

from dean_utils.polars_extras import (
    pl_scan_hive,
    pl_scan_pq,
    pl_write_pq,
)

with contextlib.suppress(ImportError):
    from dean_utils.polars_extras import pl_write_delta_append

from pathlib import Path

from dean_utils.utils.az_utils import (
    async_abfs,
    clear_messages,
    delete_message,
    get_queue_properties,
    peek_messages,
    send_message,
    update_queue,
)
from dean_utils.utils.email_utility import az_send
from dean_utils.utils.httpx import global_async_client


def error_email(func, attempts=1):
    def wrapper(*args, **kwargs):
        subject = Path.cwd()
        errors = []
        for _ in range(attempts):
            try:
                return func(*args, **kwargs)
            except Exception as err:
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
            str(subject),
            msg="\n".join(errors),
        )

    return wrapper


ACCOUNT_NAME = Literal["account_name"]
ACCOUNT_KEY = Literal["account_key"]
STORAGE_OPTIONS = dict[Literal["account_name"] | Literal["account_key"], str]


def stor_opts(
    os_env: str = "Synblob",
) -> STORAGE_OPTIONS:
    conn_str = os.environ.get(os_env)
    assert conn_str is not None
    res = {
        k: y[1]
        for x in conn_str.split(";")
        if (k := _name_key((y := x.split("=", maxsplit=1))[0])) is not None
    }
    return cast("STORAGE_OPTIONS", res)


def _name_key(x: str) -> str | None:
    if x == "AccountName":
        return "account_name"
    elif x == "AccountKey":
        return "account_key"
    else:
        return None


def stor_opts_dict(
    os_env: str = "Synblob",
) -> dict[
    Literal["storage_options"],
    STORAGE_OPTIONS,
]:
    return {"storage_options": stor_opts(os_env)}

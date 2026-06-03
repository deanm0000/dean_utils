# This file is for old backward compatibility should aim to not use it

from __future__ import annotations

import asyncio
import json
import os
from typing import TYPE_CHECKING, Literal, TypeAlias, overload

from azure.storage.queue import TextBase64EncodePolicy
from azure.storage.queue.aio import QueueServiceClient as QSC

if TYPE_CHECKING:
    from azure.storage.queue import QueueMessage

HTTPX_METHODS: TypeAlias = Literal["GET", "POST"]
if (conn_str := os.environ.get("AzureWebJobsStorage")) is not None:
    AIO_SERVE = QSC.from_connection_string(conn_str=conn_str)
elif (
    account_url := os.environ.get("AzureWebJobsStorage__queueServiceUri")
) is not None:
    try:
        from azure.identity.aio import DefaultAzureCredential

        kwargs = {}
        if (client_id := os.environ.get("AzureWebJobsStorage__clientId")) is not None:
            kwargs["managed_identity_client_id"] = client_id
        credential = DefaultAzureCredential(**kwargs)
        AIO_SERVE = QSC(account_url=account_url, credential=credential)
    except ImportError as err:
        GLOBAL_ERR = err
        AIO_SERVE = None
else:
    AIO_SERVE = None


class Queues:
    def __init__(self, conn_str: None | str = None):
        if conn_str is None and AIO_SERVE is not None:
            self.AIO = AIO_SERVE
        elif conn_str is None and AIO_SERVE is None:
            msg = "no conn str and no aio_serve"
            raise ValueError(msg)
        elif conn_str is not None:
            self.AIO = QSC.from_connection_string(conn_str=conn_str)
        else:
            msg = f"{conn_str=} {AIO_SERVE=}"
            raise ValueError(msg)


async def peek_messages(queue: str, max_messages: int | None = None, **kwargs):
    if AIO_SERVE is None:
        raise GLOBAL_ERR
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        return await aio_client.peek_messages(max_messages=max_messages)


async def get_queue_properties(queue: str, **kwargs):
    if AIO_SERVE is None:
        raise GLOBAL_ERR
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        return await aio_client.get_queue_properties()


@overload
async def send_message(
    queue: str,
    messages: list[str],
    *,
    visibility_timeout: int | None = None,
    time_to_live: int | None = None,
    timeout: int | None = None,
    **kwargs,
) -> list[QueueMessage]: ...
@overload
async def send_message(
    queue: str,
    messages: str | dict,
    *,
    visibility_timeout: int | None = None,
    time_to_live: int | None = None,
    timeout: int | None = None,
    **kwargs,
) -> QueueMessage: ...
async def send_message(
    queue: str,
    messages: list[str] | str | dict,
    *,
    visibility_timeout: int | None = None,
    time_to_live: int | None = None,
    timeout: int | None = None,
    **kwargs,
):
    if AIO_SERVE is None:
        raise GLOBAL_ERR
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        if isinstance(messages, list):
            tasks = await asyncio.gather(
                *[
                    aio_client.send_message(
                        message if isinstance(message, str) else json.dumps(message),
                        visibility_timeout=visibility_timeout,
                        time_to_live=time_to_live,
                        timeout=timeout,
                        **kwargs,
                    )
                    for message in messages
                ]
            )
            return tasks
        else:
            if not isinstance(messages, str):
                messages = json.dumps(messages)
            return await aio_client.send_message(
                messages,
                visibility_timeout=visibility_timeout,
                time_to_live=time_to_live,
                timeout=timeout,
                **kwargs,
            )


async def update_queue(
    queue,
    id,
    pop_receipt,
    message,
    *,
    visibility_timeout: int | None = None,
    timeout: int | None = None,
    **kwargs,
):
    if AIO_SERVE is None:
        raise GLOBAL_ERR
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        task = await aio_client.update_message(
            id,
            pop_receipt,
            content=message,
            visibility_timeout=visibility_timeout,
            timeout=timeout,
            **kwargs,
        )
        return task


async def delete_message(
    queue,
    id,
    pop_receipt,
    **kwargs,
):
    if AIO_SERVE is None:
        raise GLOBAL_ERR
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        task = await aio_client.delete_message(
            id,
            pop_receipt,
            **kwargs,
        )
        return task


async def clear_messages(
    queue,
    **kwargs,
):
    if AIO_SERVE is None:
        raise GLOBAL_ERR
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        task = await aio_client.clear_messages(**kwargs)
        return task

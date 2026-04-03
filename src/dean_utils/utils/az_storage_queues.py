from __future__ import annotations

import asyncio
import json
import os
from typing import (
    TYPE_CHECKING,
    overload,
)

from azure.storage.queue import TextBase64EncodePolicy
from azure.storage.queue.aio import QueueServiceClient as QSC

if TYPE_CHECKING:
    from azure.storage.queue import QueueMessage


class Queue:
    def __init__(self, conn_str: None | str = None):
        if conn_str is None:
            conn_str = os.environ.get("AzureWebJobsStorage")
        if conn_str is None:
            msg = "no conn_str"
            raise KeyError(msg)
        self.AIO_SERVE = QSC.from_connection_string(conn_str=conn_str)

    async def peek_messages(
        self, queue: str, max_messages: int | None = None, **kwargs
    ):
        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            return await aio_client.peek_messages(max_messages=max_messages, **kwargs)

    async def get_queue_properties(self, queue: str, **kwargs):
        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            return await aio_client.get_queue_properties(**kwargs)

    @overload
    async def send_message(
        self,
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
        self,
        queue: str,
        messages: str | dict,
        *,
        visibility_timeout: int | None = None,
        time_to_live: int | None = None,
        timeout: int | None = None,
        **kwargs,
    ) -> QueueMessage: ...
    async def send_message(
        self,
        queue: str,
        messages: list[str] | str | dict,
        *,
        visibility_timeout: int | None = None,
        time_to_live: int | None = None,
        timeout: int | None = None,
        **kwargs,
    ):
        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            if isinstance(messages, list):
                tasks = await asyncio.gather(
                    *[
                        aio_client.send_message(
                            message
                            if isinstance(message, str)
                            else json.dumps(message),
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
        self,
        queue,
        id,
        pop_receipt,
        message,
        *,
        visibility_timeout: int | None = None,
        timeout: int | None = None,
        **kwargs,
    ):
        async with self.AIO_SERVE.get_queue_client(
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
        self,
        queue,
        id,
        pop_receipt,
        **kwargs,
    ):
        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            task = await aio_client.delete_message(
                id,
                pop_receipt,
                **kwargs,
            )
            return task

    async def clear_messages(
        self,
        queue,
        **kwargs,
    ):
        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            task = await aio_client.clear_messages(**kwargs)
            return task

    async def retry(self, queue: str, message: str | dict, visibility_timeout: int):
        """
        Async Context handler that creates a queue message on open and then deletes it on close.

        The use case for this is an Azure Functions which might timeout. There's no try except way
        to deal with a forced timeout since the script is simply killed.
        With this, a queue message can be used to trigger a retry (or a different script entirely).

        You specify how long until the message should be acted upon, generally, 10 minutes less
        elapsed time in seconds. If the AZ Func finishes like normal then the created message gets
        deleted when this context handler is finished and nothing happens. If the AZ Func server
        kills the function during that time the queue message will become live at about the same
        time which triggers a retry.

        Usage:
        async with queue.retry(name_of_queue, message_to_queue, visibility_timeout=timeout):
            # commands
        """
        return QueueRetry(self, queue, message, visibility_timeout)


class QueueRetry:
    def __init__(
        self,
        queue: Queue,
        queue_name: str,
        message: list[str] | str | dict,
        visibility_timeout: int,
    ):
        self.queue = queue
        self.queue_name = queue_name
        self.message = message
        self.visibility_timeout = visibility_timeout

    async def __aenter__(self):
        self.queue_message = asyncio.create_task(
            self.queue.send_message(
                self.message, visibility_timeout=self.visibility_timeout
            )  # type: ignore
        )

    async def __aexit__(self, exc_type, exc_value, traceback):
        queue_message = await self.queue_message
        await self.queue.delete_message(
            self.message, queue_message["id"], queue_message["pop_receipt"]
        )

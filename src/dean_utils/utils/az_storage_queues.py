from __future__ import annotations

import asyncio
import json
import os
from typing import (
    TYPE_CHECKING,
    Any,
    overload,
)

# from azure.core.exceptions import (
#     HttpResponseError,
#     ResourceExistsError,
#     ResourceNotFoundError,
# )
if TYPE_CHECKING:
    from azure.storage.queue import QueueMessage


class Queue:
    def __init__(self, *, conn_str: None | str = None, queue: str | None = None):
        try:
            from azure.storage.queue.aio import QueueServiceClient as QSC

        except ImportError as e:
            msg = "azure-storage-queue is not installed, run `pip install azure-storage-queue` to use this functionality"
            raise ImportError(msg) from e
        self.queue = queue

        if conn_str is not None:
            self.conn_str = conn_str
        elif os.environ.get("AzureWebJobsStorage") is not None:
            self.conn_str = os.environ.get("AzureWebJobsStorage")
        else:
            self.conn_str = None
        if self.conn_str is None:
            msg = "No connection string provided for Azure Storage Queues. Please provide a connection string or set the AzureWebJobsStorage environment variable."
            raise KeyError(msg)

        self.AIO_SERVE = QSC.from_connection_string(conn_str=self.conn_str)

    def change_queue(self, queue: str):
        self.queue = queue

    def _validate(self, queue: str | None) -> str:
        if queue is None and self.queue is None:
            msg = "queue name must be provided either at the class level or the function level"
            raise ValueError(msg)
        else:
            queue = queue or self.queue
        assert queue is not None
        return queue

    async def peek_messages(
        self, queue: str | None = None, *, max_messages: int | None = None, **kwargs
    ):
        from azure.storage.queue import TextBase64EncodePolicy

        queue = self._validate(queue)

        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            return await aio_client.peek_messages(max_messages=max_messages, **kwargs)

    async def get_queue_properties(self, queue: str | None = None, **kwargs):
        from azure.storage.queue import TextBase64EncodePolicy

        queue = self._validate(queue)
        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            return await aio_client.get_queue_properties(**kwargs)

    @overload
    async def send_message(
        self,
        messages: list[str],
        *,
        queue: str | None,
        visibility_timeout: int | None = None,
        time_to_live: int | None = None,
        timeout: int | None = None,
        **kwargs,
    ) -> list[QueueMessage]: ...
    @overload
    async def send_message(
        self,
        messages: str | dict,
        *,
        queue: str | None,
        visibility_timeout: int | None = None,
        time_to_live: int | None = None,
        timeout: int | None = None,
        **kwargs,
    ) -> QueueMessage: ...

    async def send_message(
        self,
        messages: list[str] | str | dict | None,
        *,
        queue: str | None = None,
        visibility_timeout: int | None = None,
        time_to_live: int | None = None,
        timeout: int | None = None,
        **kwargs,
    ):
        from azure.storage.queue import TextBase64EncodePolicy

        queue = self._validate(queue)
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
        id: str,
        pop_receipt: str,
        message: str | dict,
        *,
        queue: str | None = None,
        visibility_timeout: int | None = None,
        timeout: int | None = None,
        **kwargs,
    ):
        from azure.storage.queue import TextBase64EncodePolicy

        queue = self._validate(queue)
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
        id: str,
        pop_receipt: str,
        *,
        queue: str | None = None,
        **kwargs,
    ):
        from azure.storage.queue import TextBase64EncodePolicy

        queue = self._validate(queue)
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
        queue: str | None = None,
        **kwargs,
    ):
        from azure.storage.queue import TextBase64EncodePolicy

        queue = self._validate(queue)
        async with self.AIO_SERVE.get_queue_client(
            queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
            task = await aio_client.clear_messages(**kwargs)
            return task

    async def retry(
        self,
        message: str | dict,
        visibility_timeout: int = 600,
        *,
        queue: str | None = None,
    ):
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
        if visibility_timeout <= 0:
            msg = "visibility_timeout must be a positive integer representing seconds until the message becomes visible"
            raise ValueError(msg)
        queue = self._validate(queue)
        return QueueRetry(self, queue, message, visibility_timeout)


class QueueRetry:
    def __init__(
        self,
        queue: Queue,
        queue_name: str,
        message: str | dict,
        visibility_timeout: int,
    ):
        self.queue = queue
        self.queue_name = queue_name
        self.message = message
        self.visibility_timeout = visibility_timeout

    async def __aenter__(self):
        self.queue_message = asyncio.create_task(
            self.queue.send_message(
                self.message,
                queue=self.queue_name,
                visibility_timeout=self.visibility_timeout,
            )
        )

    async def __aexit__(self, exc_type, exc_value, traceback):
        queue_message = await self.queue_message
        await self.queue.delete_message(
            queue=self.queue_name,
            id=queue_message["id"],
            pop_receipt=queue_message["pop_receipt"],
        )

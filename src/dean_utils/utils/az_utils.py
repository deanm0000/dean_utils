import re
import os
from azure.storage.queue.aio import QueueServiceClient as QSC
from azure.storage.queue import TextBase64EncodePolicy
import azure.storage.blob as asb
from azure.storage.blob.aio import BlobClient
from azure.storage.blob import BlobBlock
from azure.core.exceptions import HttpResponseError
import asyncio
from typing import List, Optional
import fsspec
import httpx
from datetime import datetime, timedelta, timezone
from typing import TypeAlias, Literal
from uuid import uuid4
import json

HTTPX_METHODS: TypeAlias = Literal["GET", "POST"]
AIO_SERVE = QSC.from_connection_string(conn_str=os.environ["AzureWebJobsStorage"])


def def_cos(db_name, client_name):
    try:
        from azure.cosmos.aio import CosmosClient

        return (
            CosmosClient(
                re.search(
                    "(?<=AccountEndpoint=).+?(?=;)", os.environ["cosmos"]
                ).group(),
                {
                    "masterKey": re.search(
                        "(?<=AccountKey=).+?(?=$)", os.environ["cosmos"]
                    ).group()
                },
            )
            .get_database_client(db_name)
            .get_container_client(client_name)
        )
    except ImportError:
        raise ImportError("MS's cosmos sdk not installed")


async def cos_query_all(cosdb, QRY):
    if "offset" in QRY and "limit" in QRY:
        raise ValueError(
            "Safe query uses offset and limit for queries so original query can't use them"
        )
    n = 1
    returns = []
    while True:
        SUBQRY = f"{QRY} offset {n} limit 1000"
        this_bunch = [x async for x in cosdb.query_items(SUBQRY)]
        returns.extend(this_bunch)
        if len(this_bunch) < 1000:
            break
        else:
            n += 1000
    return returns


async def send_message(
    queue: str,
    messages: List[str] | str | dict,
    *,
    visibility_timeout: int | None = None,
    time_to_live: int | None = None,
    timeout: int | None = None,
    **kwargs,
):
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        if isinstance(messages, list):
            tasks = []
            for message in messages:
                if not isinstance(message, str):
                    message = json.dumps(message)
                asyncio.create_task(
                    aio_client.send_message(
                        message,
                        visibility_timeout=visibility_timeout,
                        time_to_live=time_to_live,
                        timeout=timeout,
                        **kwargs,
                    )
                )

            await asyncio.wait(tasks)
            return tasks
        else:
            if not isinstance(messages, str):
                messages = json.dumps(messages)
            return await aio_client.send_message(messages)


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
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
    ) as aio_client:
        task = await aio_client.delete_message(
            id,
            pop_receipt,
            **kwargs,
        )
        return task


class async_abfs:
    def __init__(self, connection_string=os.environ["Synblob"]):
        self.connection_string = connection_string
        self.sync = fsspec.filesystem("abfss", connection_string=self.connection_string)
        key_conv = {"AccountName": "account_name", "AccountKey": "account_key"}
        stor = {
            (splt := x.split("=", 1))[0]: splt[1]
            for x in self.connection_string.split(";")
        }
        stor = {
            key_conv[key]: val for key, val in stor.items() if key in key_conv.keys()
        }
        self.stor = stor

    async def stream_dl(
        self,
        client: httpx.AsyncClient,
        method: HTTPX_METHODS,
        url: str,
        path: str,
        /,
        recurs=False,
        **httpx_extras,
    ) -> None:
        """
        Help on method stream_dl

        async stream_dl(client, method, url, **httpx_extras)
            Download file streaming in chunks in async as downloader and to a Blob

            Parameters
            ----------
            client: httpx.AsyncClient
                The httpx Async Client object to use
            method:
                The HTTP method whether GET or POST
            url:
                The URL to download
            path:
                The full path to Azure file being saved
            **httpx_extras
                Any extra arguments to be sent to client.stream
        """
        async with BlobClient.from_connection_string(
            self.connection_string, *(path.split("/", maxsplit=1))
        ) as target, client.stream(method, url, **httpx_extras) as resp:
            resp.raise_for_status()
            block_list = []
            async for chunk in resp.aiter_bytes():
                block_id = uuid4().hex
                try:
                    await target.stage_block(block_id=block_id, data=chunk)
                except HttpResponseError as err:
                    if "The specified blob or block content is invalid." not in str(
                        err
                    ):
                        raise err
                    await asyncio.sleep(1)
                    await target.commit_block_list([])
                    await target.delete_blob()
                    if recurs is False:
                        await self.stream_dl(
                            client,
                            method,
                            url,
                            path,
                            recurs=True,
                            **httpx_extras,
                        )
                    else:
                        raise err
                block_list.append(BlobBlock(block_id=block_id))
            await target.commit_block_list(block_list)

    async def walk(self, path: str, maxdepth=None, **kwargs):
        """
        Help on method _async_walk in module adlfs.spec:

        async _async_walk(path: str, maxdepth=None, **kwargs) method of adlfs.spec.AzureBlobFileSystem instance
            Return all files belows path

            List all files, recursing into subdirectories; output is iterator-style,
            like ``os.walk()``. For a simple list of files, ``find()`` is available.

            Note that the "files" outputted will include anything that is not
            a directory, such as links.

            Parameters
            ----------
            path: str
                Root to recurse into

            maxdepth: int
                Maximum recursion depth. None means limitless, but not recommended
                on link-based file-systems.

            **kwargs are passed to ``ls``
        """
        this_fs = fsspec.filesystem(
            "abfss", connection_string=self.connection_string, asyncronous=True
        )
        return [x async for x in this_fs._async_walk(path, maxdepth, **kwargs)]

    async def exists(self, path: str):
        """
        Help on method _exists in module adlfs.spec:

        async _exists(path) method of adlfs.spec.AzureBlobFileSystem instance
            Is there a file at the given path
        """
        this_fs = fsspec.filesystem(
            "abfss", connection_string=self.connection_string, asyncronous=True
        )
        return await this_fs._exists(path)

    async def details(
        self,
        contents,
        delimiter="/",
        return_glob: bool = False,
        target_path="",
        version_id: Optional[str] = None,
        versions: bool = False,
        **kwargs,
    ):
        """
        Help on method _details in module adlfs.spec:

        async _details(contents, delimiter='/', return_glob: bool = False, target_path='', version_id: Optional[str] = None, versions: bool = False, **kwargs) method of adlfs.spec.AzureBlobFileSystem instance
            Return a list of dictionaries of specifying details about the contents

            Parameters
            ----------
            contents

            delimiter: str
                Delimiter used to separate containers and files

            return_glob: bool

            version_id: str
                Specific target version to be returned

            versions: bool
                If True, return all versions

            Returns
            -------
            List of dicts
                Returns details about the contents, such as name, size and type
        """
        this_fs = fsspec.filesystem(
            "abfss", connection_string=self.connection_string, asyncronous=True
        )
        return await this_fs._details(
            contents,
            delimiter=delimiter,
            return_glob=return_glob,
            target_path=target_path,
            version_id=version_id,
            versions=versions,
            **kwargs,
        )

    async def put_file(
        self,
        lpath,
        rpath,
        delimiter="/",
        overwrite=True,
        callback=None,
        max_concurrency=None,
        **kwargs,
    ):
        """
        Copy single file to remote

        :param lpath: Path to local file
        :param rpath: Path to remote file
        :param delimitier: Filepath delimiter
        :param overwrite: Boolean (True). Whether to overwrite any existing file
            (True) or raise if one already exists (False).
        """
        this_fs = fsspec.filesystem(
            "abfss", connection_string=self.connection_string, asyncronous=True
        )
        return await this_fs._put_file(
            lpath,
            rpath=rpath,
            delimiter=delimiter,
            overwrite=overwrite,
            callback=callback,
            max_concurrency=max_concurrency,
        )

    async def ls(
        self,
        path: str,
        detail: bool = False,
        delimiter: str = "/",
        return_glob: bool = False,
        version_id: Optional[str] = None,
        versions: bool = False,
        **kwargs,
    ):
        """
        Help on method _ls in module adlfs.spec:

        async _ls(path: str, detail: bool = False, invalidate_cache: bool = False, delimiter: str = '/', return_glob: bool = False, version_id: Optional[str] = None, versions: bool = False, **kwargs) method of adlfs.spec.AzureBlobFileSystem instance
            Create a list of blob names from a blob container

            Parameters
            ----------
            path: str
                Path to an Azure Blob with its container name

            detail: bool
                If False, return a list of blob names, else a list of dictionaries with blob details

            delimiter: str
                Delimiter used to split paths

            version_id: str
                Specific blob version to list

            versions: bool
                If True, list all versions

            return_glob: bool
        """
        this_fs = fsspec.filesystem(
            "abfss", connection_string=self.connection_string, asyncronous=True
        )
        return await this_fs._ls(
            path,
            detail=detail,
            delimiter=delimiter,
            return_glob=return_glob,
            version_id=version_id,
            versions=versions,
            invalidate_cache=True,
        )

    async def rm(
        self,
        path,
        recursive=False,
        maxdepth=None,
        delimiter="/",
        expand_path=True,
        **kwargs,
    ):
        """
        Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            Defaults to False.
            If file(s) are directories, recursively delete contents and then
            also remove the directory.
            Only used if `expand_path`.

        maxdepth: int or None
            Defaults to None.
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
            Only used if `expand_path`.
        expand_path: bool
            Defaults to True.
            If False, `self._expand_path` call will be skipped. This is more
            efficient when you don't need the operation.
        """
        this_fs = fsspec.filesystem(
            "abfss", connection_string=self.connection_string, asyncronous=True
        )
        return await this_fs._rm(
            path=path,
            recursive=recursive,
            maxdepth=maxdepth,
            delimiter=delimiter,
            expand_path=expand_path,
        )

    def make_sas_link(self, filepath, expiry=None, write=False):
        account_dict = {
            x.split("=", 1)[0]: x.split("=", 1)[1]
            for x in self.connection_string.split(";")
        }
        if write is True and expiry is None:
            expiry = datetime.now(timezone.utc) + timedelta(minutes=30)
        elif write is False and expiry is None:
            expiry = datetime(2050, 1, 1, tzinfo=timezone.utc)
        if isinstance(expiry, str):
            expiry = datetime.fromisoformat(expiry)
        sas = asb.generate_blob_sas(
            account_name=account_dict["AccountName"],
            account_key=account_dict["AccountKey"],
            container_name=filepath.split("/", 1)[0],
            blob_name=filepath.split("/", 1)[1],
            permission=asb.BlobSasPermissions(read=True, write=write),
            expiry=expiry,
        )
        return f"https://{account_dict['AccountName']}.blob.core.windows.net/{filepath}?{sas}"

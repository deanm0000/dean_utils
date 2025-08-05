from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Literal, TypeAlias, cast, overload
from uuid import uuid4

import azure.storage.blob as asb
import fsspec
from aiopath import AsyncPath
from azure.core.exceptions import HttpResponseError
from azure.storage.blob import BlobBlock
from azure.storage.blob.aio import BlobClient
from azure.storage.queue import TextBase64EncodePolicy
from azure.storage.queue.aio import QueueServiceClient as QSC

if TYPE_CHECKING:
    import httpx
    from azure.storage.blob._models import BlobProperties
    from azure.storage.blob.aio import StorageStreamDownloader
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


class async_abfs:
    def __init__(self, connection_string=os.environ["Synblob"]):
        self.connection_string = connection_string
        self.sync = fsspec.filesystem("abfss", connection_string=self.connection_string)
        key_conv = {"AccountName": "account_name", "AccountKey": "account_key"}
        stor = {
            (splt := x.split("=", 1))[0]: splt[1]
            for x in self.connection_string.split(";")
        }
        stor = {key_conv[key]: val for key, val in stor.items() if key in key_conv}
        self.stor = stor

    async def get_blob_properties(self, path, **kwargs: Any) -> BlobProperties:
        r"""
            Returns all metadata for the blob.

        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.aio.BlobLeaseClient or str
        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to get properties.

            .. versionadded:: 12.4.0

            This keyword argument was introduced in API version '2019-12-12'.

        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``"\"tagname\"='my tag'"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network
            timesouts see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :returns: BlobProperties
        :rtype: ~azure.storage.blob.BlobProperties

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common_async.py
                :start-after: [START get_blob_properties]
                :end-before: [END get_blob_properties]
                :language: python
                :dedent: 12
                :caption: Getting the properties for a blob.

            md5 is at .content_settings.content_md5.hex()
        """
        async with (
            BlobClient.from_connection_string(
                self.connection_string, *(path.split("/", maxsplit=1))
            ) as target,
        ):
            return await target.get_blob_properties(**kwargs)

    async def from_url(
        self,
        source_url: str,
        path: str,
        metadata: dict[str, str] | None = None,
        *,
        incremental_copy: bool = False,
        **kwargs,
    ) -> dict[str, str | datetime]:
        r"""
            Copies a blob from the given URL.

            Args:
        :param str source_url:
            A URL of up to 2 KB in length that specifies a file or blob.
            The value should be URL-encoded as it would appear in a request URI.
            If the source is in another account, the source must either be public
            or must be authenticated via a shared access signature. If the source
            is public, no authentication is required.

        Examples
        --------
            https://myaccount.blob.core.windows.net/mycontainer/myblob

            https://myaccount.blob.core.windows.net/mycontainer/myblob?snapshot=<DateTime>

            https://otheraccount.blob.core.windows.net/mycontainer/myblob?sastoken
        :param path:
            The destination path to which the source_url will be copied
        :param metadata:
            Name-value pairs associated with the blob as metadata. If no name-value
            pairs are specified, the operation will copy the metadata from the
            source blob or file to the destination blob. If one or more name-value
            pairs are specified, the destination blob is created with the specified
            metadata, and metadata is not copied from the source blob or file.
        :type metadata: dict(str, str)
        :param bool incremental_copy:
            Copies the snapshot of the source page blob to a destination page blob.
            The snapshot is copied such that only the differential changes between
            the previously copied snapshot are transferred to the destination.
            The copied snapshots are complete copies of the original snapshot and
            can be read or copied from as usual. Defaults to False.
        :keyword tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters,
            digits (0-9), space (' '), plus (+), minus (-), period (.), solidus (/), colon (:),
            equals (=), underscore (_).

            The (case-sensitive) literal "COPY" can instead be passed to copy tags from the source
            blob.
            This option is only available when `incremental_copy=False` and `requires_sync=True`.

            .. versionadded:: 12.4.0

        :paramtype tags: dict(str, str) or Literal["COPY"]
        :keyword ~azure.storage.blob.ImmutabilityPolicy immutability_policy:
            Specifies the immutability policy of a blob, blob snapshot or blob version.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword bool legal_hold:
            Specified if a legal hold should be set on the blob.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword ~datetime.datetime source_if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only if the source
            blob has been modified since the specified date/time.
        :keyword ~datetime.datetime source_if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only if the source blob
            has not been modified since the specified date/time.
        :keyword str source_etag:
            The source ETag value, or the wildcard character (*). Used to check if the resource has
            changed, and act according to the condition specified by the `match_condition`
            parameter.
        :keyword ~azure.core.MatchConditions source_match_condition:
            The source match condition to use upon the etag.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only
            if the destination blob has been modified since the specified date/time.
            If the destination blob has not been modified, the Blob service returns
            status code 412 (Precondition Failed).
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only
            if the destination blob has not been modified since the specified
            date/time. If the destination blob has been modified, the Blob service
            returns status code 412 (Precondition Failed).
        :keyword str etag:
            The destination ETag value, or the wildcard character (*). Used to check if the resource
            has changed, and act according to the condition specified by the `match_condition`
            parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The destination match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``"\"tagname\"='my tag'"``

            .. versionadded:: 12.4.0

        :keyword destination_lease:
            The lease ID specified for this header must match the lease ID of the
            destination blob. If the request does not include the lease ID or it is not
            valid, the operation fails with status code 412 (Precondition Failed).
        :paramtype destination_lease: ~azure.storage.blob.aio.BlobLeaseClient or str
        :keyword source_lease:
            Specify this to perform the Copy Blob operation only if
            the lease ID given matches the active lease ID of the source blob.
        :paramtype source_lease: ~azure.storage.blob.aio.BlobLeaseClient or str
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network
            timesouts see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword ~azure.storage.blob.PremiumPageBlobTier premium_page_blob_tier:
            A page blob tier value to set the blob to. The tier correlates to the size of the
            blob and number of allowed IOPS. This is only applicable to page blobs on
            premium storage accounts.
        :keyword ~azure.storage.blob.StandardBlobTier standard_blob_tier:
            A standard blob tier value to set the blob to. For this version of the library,
            this is only applicable to block blobs on standard storage accounts.
        :keyword ~azure.storage.blob.RehydratePriority rehydrate_priority:
            Indicates the priority with which to rehydrate an archived blob
        :keyword bool seal_destination_blob:
            Seal the destination append blob. This operation is only for append blob.

            .. versionadded:: 12.4.0

        :keyword bool requires_sync:
            Enforces that the service will not return a response until the copy is complete.
        :keyword str source_authorization:
            Authenticate as a service principal using a client secret to access a source blob.
            Ensure "bearer " is the prefix of the source_authorization string. This option is only
            available when `incremental_copy` is set to False and `requires_sync` is set to True.

            .. versionadded:: 12.9.0

        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the sync copied blob. An
            encryption scope can be created using the Management API and referenced here by name. If
            a default encryption scope has been defined at the container, this value will override
            it if the container-level scope is configured to allow overrides. Otherwise an error
            will be raised.

            .. versionadded:: 12.10.0

        :returns: A dictionary of copy properties (etag, last_modified, copy_id, copy_status).
        :rtype: dict[str, Union[str, ~datetime.datetime]]

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common_async.py
                :start-after: [START copy_blob_from_url]
                :end-before: [END copy_blob_from_url]
                :language: python
                :dedent: 16
                :caption: Copy a blob from a URL.



        Returns
        -------
                dict[str, str | datetime]: _description_
        """
        async with (
            BlobClient.from_connection_string(
                self.connection_string, *(path.split("/", maxsplit=1))
            ) as target,
        ):
            return await target.start_copy_from_url(
                source_url, metadata, incremental_copy=incremental_copy, **kwargs
            )

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
        stream_dl will stream the contents of a url to a path in the cloud given an httpx Client.

        async stream_dl(client, method, url, path, recurs, **httpx_extras)
            Download file streaming in chunks in async as downloader and to a Blob

        Args:
            client: httpx.AsyncClient
                The httpx Async Client object to use
            method:
                The HTTP method whether GET or POST
            url:
                The URL to download
            path:
                The full path to Azure file being saved
            recurs:
                To try again recursively
            httpx_extras
                Any extra arguments to be sent to client.stream
        """
        async with (
            BlobClient.from_connection_string(
                self.connection_string, *(path.split("/", maxsplit=1))
            ) as target,
            client.stream(method, url, **httpx_extras) as resp,
        ):
            resp.raise_for_status()
            block_list = []
            async for chunk in resp.aiter_bytes():
                chunk = cast("IO", chunk)
                block_id = uuid4().hex
                try:
                    await target.stage_block(block_id=block_id, data=chunk)
                except HttpResponseError as err:
                    if "The specified blob or block content is invalid." not in str(
                        err
                    ):
                        raise
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
                        raise
                block_list.append(BlobBlock(block_id=block_id))
            await target.commit_block_list(block_list)

    async def stream_up(
        self,
        local_path: str | Path | AsyncPath,
        remote_path: str,
        size: int = 16384,
        /,
        recurs=False,
    ) -> None:
        """
        Help on method stream_up.

        async stream_up(local_path, remote_path, size, recurs)
            Download file streaming in chunks in async as downloader and to a Blob

        Args:
            local_path:
                The full path to local path as str or Path
            remote_path:
                The full path to remote path as str
            size:
                The number of bytes read per iteration in read
            recurs:
                To try again recursively
        """
        if isinstance(local_path, (str, Path)):
            local_path = AsyncPath(local_path)
        async with (
            BlobClient.from_connection_string(
                self.connection_string, *(remote_path.split("/", maxsplit=1))
            ) as target,
            local_path.open("rb") as src,
        ):
            block_list = []
            while True:
                chunk = await src.read(size)
                chunk = cast("IO", chunk)
                if not chunk:
                    break
                block_id = uuid4().hex
                try:
                    await target.stage_block(block_id=block_id, data=chunk)
                except HttpResponseError as err:
                    if "The specified blob or block content is invalid." not in str(
                        err
                    ):
                        raise
                    await asyncio.sleep(1)
                    await target.commit_block_list([])
                    await target.delete_blob()
                    if recurs is False:
                        await self.stream_up(
                            local_path,
                            remote_path,
                            recurs=True,
                        )
                    else:
                        raise
                block_list.append(BlobBlock(block_id=block_id))
            await target.commit_block_list(block_list)

    async def walk(self, path: str, maxdepth=None, **kwargs):
        """
        Help on method _async_walk in module adlfs.spec.

        async _async_walk(path: str, maxdepth=None, **kwargs) method of AzureBlobFileSystem instance
            Return all files belows path

            list all files, recursing into subdirectories; output is iterator-style,
            like ``os.walk()``. For a simple list of files, ``find()`` is available.

            Note that the "files" outputted will include anything that is not
            a directory, such as links.

        Args:
            path: str
                Root to recurse into

            maxdepth: int
                Maximum recursion depth. None means limitless, but not recommended
                on link-based file-systems.

            kwargs:
                dict of args passed to ``ls``
        """
        this_fs = fsspec.filesystem(
            "abfss", connection_string=self.connection_string, asyncronous=True
        )
        return [x async for x in this_fs._async_walk(path, maxdepth, **kwargs)]

    async def exists(self, path: str):
        """
        Help on method _exists in module adlfs.spec.

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
        *,
        return_glob: bool = False,
        target_path="",
        version_id: str | None = None,
        versions: bool = False,
        **kwargs,
    ):
        """
        Help on method _details in module adlfs.spec.

        async _details(contents, delimiter='/', return_glob: bool = False, target_path='',
        version_id: Optional[str] = None, versions: bool = False, **kwargs) method of
            AzureBlobFileSystem instance
            Return a list of dictionaries of specifying details about the contents

        Args:
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
            list of dicts
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
        Copy single file to remote.

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
        *,
        detail: bool = False,
        delimiter: str = "/",
        return_glob: bool = False,
        version_id: str | None = None,
        versions: bool = False,
        **kwargs,
    ):
        """
        Help on method _ls in module adlfs.spec.

        async _ls(path: str, detail: bool = False, invalidate_cache: bool = False,
        delimiter: str = '/', return_glob: bool = False, version_id: Optional[str] = None,
        versions: bool = False, **kwargs) method of adlfs.spec.AzureBlobFileSystem instance
            Create a list of blob names from a blob container

        Args:
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

        Args:
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

    def make_sas_link(
        self,
        filepath: str,
        expiry: datetime | None = None,
        *,
        write: bool = False,
        content_disposition_filename: str | None = None,
    ):
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
        if content_disposition_filename is None:
            content_disposition = None
        else:
            content_disposition = (
                f'attachment; filename="{content_disposition_filename}"'
            )
        sas = asb.generate_blob_sas(
            account_name=account_dict["AccountName"],
            account_key=account_dict["AccountKey"],
            container_name=filepath.split("/", 1)[0],
            blob_name=filepath.split("/", 1)[1],
            permission=asb.BlobSasPermissions(read=True, write=write),
            expiry=expiry,
            content_disposition=content_disposition,
        )
        return f"https://{account_dict['AccountName']}.blob.core.windows.net/{filepath}?{sas}"

    async def stream(self, path: str) -> StorageStreamDownloader[bytes]:
        blob = BlobClient.from_connection_string(
            self.connection_string, *(path.split("/", maxsplit=1))
        )
        stream = await blob.download_blob()
        return stream

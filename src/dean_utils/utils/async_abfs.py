from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Literal,
    TypeAlias,
    cast,
    overload,
)
from uuid import uuid4

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    import httpx
    from azure.storage.blob import BlobBlock
    from azure.storage.blob._models import BlobProperties
    from azure.storage.blob.aio import BlobClient

HTTPX_METHODS: TypeAlias = Literal["GET", "POST"]


@overload
def _clean_path(path: str) -> str: ...
@overload
def _clean_path(path: Sequence[str]) -> Sequence[str]: ...
def _clean_path(path: str | Sequence[str]) -> str | Sequence[str]:
    """
    Clean a path by removing abfs:// and @ account name.

    Parameters
    ----------
    path: str | Sequence[str]
        The path to clean.

    Returns
    -------
    str | Sequence[str]
        The cleaned path.
    """
    if isinstance(path, str):
        path = path.split("://", maxsplit=1)[-1]
        at_loc = path.find("@")

        if at_loc == -1:
            return path
        slash_loc = path.find("/", at_loc)
        if slash_loc == -1:
            return path[:at_loc]
        return path[:at_loc] + path[slash_loc:]
    elif isinstance(path, Sequence):
        return [_clean_path(p) for p in path]


class abfs_writer:
    def __init__(self, connection_string: str, path: str):
        self.connection_string = connection_string
        path = _clean_path(path)
        self.path = path
        self._write_json = False

    async def __aenter__(self):
        from azure.storage.blob.aio import BlobClient

        self.blob_client = BlobClient.from_connection_string(
            self.connection_string, *(self.path.split("/", maxsplit=1))
        )
        self.block_list = []
        return self

    async def write(self, chunk: bytes | str):
        """Stage a bytes or text chunk for upload to the blob."""
        from azure.storage.blob import BlobBlock

        if self._write_json:
            msg = "can't write on top of existing json"
            raise ValueError(msg)
        block_id = uuid4().hex
        if isinstance(chunk, str):
            chunk = chunk.encode("utf8")
        await self.blob_client.stage_block(block_id=block_id, data=chunk)

        self.block_list.append(BlobBlock(block_id=block_id))

    async def write_json(self, data: dict | list):
        """
        Serialize JSON data and stage it as the blob contents.

        Parameters
        ----------
        data: dict | list
            JSON-serializable data to upload.
        """
        if len(self.block_list) > 0:
            msg = "can't write json on top of other writes"
            raise ValueError(msg)
        try:
            import orjson

            dumps = orjson.dumps
        except ModuleNotFoundError:
            import json

            def dumps(__obj: Any) -> bytes:
                return json.dumps(__obj).encode("utf8")

        chunk = dumps(data)
        await self.write(chunk)

        self._write_json = True

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.blob_client.commit_block_list(self.block_list)
        await self.blob_client.close()


class async_abfs:
    def __init__(
        self, fsspec_protocol: str = "abfss", connection_string: str | None = None
    ):
        import fsspec

        connection_string = connection_string or os.environ.get("Synblob")
        if connection_string is None:
            msg = "no connection string provided and no Synblob env var"
            raise ValueError(msg)
        self.fsspec_protocol = fsspec_protocol
        self.connection_string = connection_string
        self.sync = fsspec.filesystem(
            fsspec_protocol, connection_string=self.connection_string
        )
        key_conv = {"AccountName": "account_name", "AccountKey": "account_key"}
        stor = {
            (splt := x.split("=", 1))[0]: splt[1]
            for x in self.connection_string.split(";")
        }
        stor = {key_conv[key]: val for key, val in stor.items() if key in key_conv}
        self.stor = stor

    async def get_blob_properties(self, path: str, **kwargs: Any) -> BlobProperties:
        """
        Return all metadata for a blob.

        Parameters
        ----------
        path: str
            The blob path.
        **kwargs: Any
            Keyword arguments forwarded to
            :meth:`azure.storage.blob.aio.BlobClient.get_blob_properties`.

        Returns
        -------
        BlobProperties
            The blob metadata and properties.
        """
        from azure.storage.blob.aio import BlobClient

        path = _clean_path(path)
        async with (
            BlobClient.from_connection_string(
                self.connection_string, *(path.split("/", maxsplit=1))
            ) as target,
        ):
            return await target.get_blob_properties(**kwargs)

    def pq_unique_values(self, path: str | Sequence[str], column: str) -> list[str]:
        """
        Return unique values from a parquet column.

        The function inspects parquet row-group statistics and expects each row
        group to contain a single value for the requested column (that is,
        statistics min and max are equal). If any row groups don't have min=max
        then this will raise a MinMaxNotEqualError.

        Parameters
        ----------
        path: str | Sequence[str]
            Local/remote parquet path understood by the configured filesystem.
        column: str
            Column name to inspect.

        Returns
        -------
        list[str]
            Unique values found across row groups, preserving first-seen order.

        Raises
        ------
        ColumnNotExistError
            If ``column`` is not present in the parquet schema.
        MinMaxNotEqualError
            If any row group has non-constant values for ``column``
            (``min != max``).
        """
        path = _clean_path(path)

        return list(self.pq_unique_items(path, column).keys())

    def pq_unique_items(
        self, path: str | Sequence[str], column: str
    ) -> dict[str, list[int]]:
        """
        Map each unique parquet column value to row-group indices.

        The function inspects parquet row-group statistics and expects each row
        group to contain a single value for the requested column (that is,
        statistics min and max are equal). If any row groups don't have min=max
        then this will raise a MinMaxNotEqualError.

        Parameters
        ----------
        path: str
            Local/remote parquet path understood by the configured filesystem.
        column: str
            Column name to inspect.

        Returns
        -------
        dict[str, list[int]]
            Keys are unique column values and values are row-group indices where
            each value appears.

        Raises
        ------
        ColumnNotExistError
            If ``column`` is not present in the parquet schema.
        MinMaxNotEqualError
            If any row group has non-constant values for ``column``
            (``min != max``).
        """
        path = _clean_path(path)
        try:
            from pyarrow import parquet as pq
        except ImportError as e:
            msg = "pyarrow is not installed, run `pip install pyarrow` to use this functionality"
            raise ImportError(msg) from e

        if isinstance(path, str):
            path = [path]
        unique_items = {}
        for p in path:
            if len(splt := p.split(":")) > 1:
                p = splt[1]
            pq_file = pq.ParquetFile(p, filesystem=self.sync)
            n_groups = pq_file.num_row_groups
            try:
                col_indx = next(
                    i for i, name in enumerate(pq_file.schema.names) if name == column
                )
            except StopIteration:
                msg = f"Column '{column}' does not exist in parquet file"
                raise ColumnNotExistError(msg) from None

            for i in range(n_groups):
                stats = pq_file.metadata.row_group(i).column(col_indx).statistics
                if stats.min != stats.max:
                    msg = (
                        f"Row group {i} column '{column}' has min {stats.min} != max {stats.max}"
                        if len(path) == 1
                        else f"Parquet file '{p}' row group {i} column '{column}' has min {stats.min} != max {stats.max}"
                    )
                    raise MinMaxNotEqualError(msg)
                if stats.min not in unique_items:
                    unique_items[stats.min] = [i]
                else:
                    unique_items[stats.min].append(i)

        return unique_items

    async def from_url(
        self,
        source_url: str,
        path: str,
        metadata: dict[str, str] | None = None,
        *,
        incremental_copy: bool = False,
        **kwargs,
    ) -> dict[str, str | datetime]:
        """
        Copy a blob from a URL to a destination path.

        Parameters
        ----------
        source_url: str
            URL of the source blob or file. It must be URL-encoded as it would
            appear in a request URI.
        path: str
            Destination blob path.
        metadata: dict[str, str] | None, default=None
            Metadata for the destination blob. When omitted, metadata is copied
            from the source.
        incremental_copy: bool, default=False
            Whether to copy only changes from a source page-blob snapshot.
        **kwargs: Any
            Keyword arguments forwarded to
            :meth:`azure.storage.blob.aio.BlobClient.start_copy_from_url`.

        Returns
        -------
        dict[str, str | datetime]
            Copy properties, including the ETag, last-modified time, copy ID,
            and copy status.
        """
        from azure.storage.blob.aio import BlobClient

        path = _clean_path(path)
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
        Stream an HTTP response to a remote blob.

        Parameters
        ----------
        client: httpx.AsyncClient
            HTTP client used to stream the response.
        method: HTTPX_METHODS
            HTTP method to use.
        url: str
            URL to download.
        path: str
            Destination blob path.
        recurs: bool, default=False
            Reserved for recursive retry compatibility.
        **httpx_extras: Any
            Additional keyword arguments passed to ``client.stream``.
        """
        from azure.storage.blob.aio import BlobClient

        path = _clean_path(path)
        async with (
            BlobClient.from_connection_string(
                self.connection_string, *(path.split("/", maxsplit=1))
            ) as target,
            client.stream(method, url, **httpx_extras) as resp,
        ):
            resp.raise_for_status()
            block_list = []
            tasks = set()
            accum = bytearray()
            async for chunk in resp.aiter_bytes():
                accum.extend(chunk)
                if len(accum) >= 256000:
                    _block_task(tasks, target, block_list, bytes(accum))
                    accum = bytearray()
            if len(accum) > 0:
                _block_task(tasks, target, block_list, bytes(accum))
            await asyncio.wait(tasks)
            await target.commit_block_list(block_list)

    async def stream_up(
        self,
        local_path: str | Path,
        remote_path: str,
        size: int = 16384,
        /,
        recurs=False,
    ) -> None:
        """
        Stream a local file to a remote blob.

        Parameters
        ----------
        local_path: str | Path
            Local file path.
        remote_path: str
            Destination blob path.
        size: int, default=16384
            Number of bytes read per upload block.
        recurs: bool, default=False
            Whether this call is a retry after an invalid block error.
        """
        if isinstance(local_path, str):
            local_path = Path(local_path)
        remote_path = _clean_path(remote_path)
        from azure.core.exceptions import HttpResponseError
        from azure.storage.blob import BlobBlock
        from azure.storage.blob.aio import BlobClient

        with local_path.open("rb") as src:
            async with BlobClient.from_connection_string(
                self.connection_string, *(remote_path.split("/", maxsplit=1))
            ) as target:
                block_list = []
                while True:
                    chunk = src.read(size)
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
        Recursively list files and directories below a path.

        Parameters
        ----------
        path: str
            Root path to recurse into.
        maxdepth: int | None, default=None
            Maximum recursion depth. ``None`` has no limit.
        **kwargs: Any
            Additional arguments passed to the underlying filesystem walk.
        """
        import fsspec

        path = _clean_path(path)
        this_fs = fsspec.filesystem(
            self.fsspec_protocol,
            connection_string=self.connection_string,
            asyncronous=True,
        )
        return [x async for x in this_fs._async_walk(path, maxdepth, **kwargs)]

    async def exists(self, path: str):
        """
        Check whether a remote blob exists.

        Parameters
        ----------
        path: str
            Blob path to check.

        Returns
        -------
        bool
            Whether the blob exists.
        """
        import fsspec

        path = _clean_path(path)
        this_fs = fsspec.filesystem(
            self.fsspec_protocol,
            connection_string=self.connection_string,
            asyncronous=True,
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
        Return details about filesystem contents.

        Parameters
        ----------
        contents: Any
            Filesystem contents to inspect.
        delimiter: str, default="/"
            Delimiter used to separate containers and files.
        return_glob: bool, default=False
            Whether ``contents`` represents a glob expression.
        target_path: str, default=""
            Target path used to resolve the details request.
        version_id: str | None, default=None
            Specific blob version to return.
        versions: bool, default=False
            Whether to return all blob versions.
        **kwargs: Any
            Additional arguments passed to the underlying filesystem method.

        Returns
        -------
        list[dict[str, Any]]
            Details such as name, size, and type.
        """
        import fsspec

        this_fs = fsspec.filesystem(
            self.fsspec_protocol,
            connection_string=self.connection_string,
            asyncronous=True,
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
        Copy a single local file to remote storage.

        Parameters
        ----------
        lpath: Any
            Local file path.
        rpath: Any
            Remote destination path.
        delimiter: str, default="/"
            File path delimiter.
        overwrite: bool, default=True
            Whether to replace an existing remote file.
        callback: Any, default=None
            Progress callback passed to the underlying filesystem method.
        max_concurrency: int | None, default=None
            Maximum concurrent upload operations.
        **kwargs: Any
            Additional arguments accepted by the underlying filesystem method.
        """
        import fsspec

        rpath = _clean_path(rpath)
        this_fs = fsspec.filesystem(
            self.fsspec_protocol,
            connection_string=self.connection_string,
            asyncronous=True,
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
        List blobs at a path.

        Parameters
        ----------
        path: str
            Path to an Azure blob, including its container name.
        detail: bool, default=False
            Whether to return blob detail dictionaries instead of names.
        delimiter: str, default="/"
            Delimiter used to split paths.
        return_glob: bool, default=False
            Whether ``path`` is a glob expression.
        version_id: str | None, default=None
            Specific blob version to list.
        versions: bool, default=False
            Whether to list all versions.
        **kwargs: Any
            Additional arguments passed to the underlying filesystem method.
        """
        import fsspec

        path = _clean_path(path)
        this_fs = fsspec.filesystem(
            self.fsspec_protocol,
            connection_string=self.connection_string,
            asyncronous=True,
        )
        return await this_fs._ls(
            path,
            detail=detail,
            delimiter=delimiter,
            return_glob=return_glob,
            version_id=version_id,
            versions=versions,
            invalidate_cache=True,
            **kwargs,
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
        Delete files or directories.

        Parameters
        ----------
        path: str | Sequence[str]
            File(s) to delete.
        recursive: bool, default=False
            Whether to delete directory contents recursively.
        maxdepth: int | None, default=None
            Maximum recursion depth when ``recursive`` is enabled.
        delimiter: str, default="/"
            File path delimiter.
        expand_path: bool, default=True
            Whether to expand paths before deletion.
        **kwargs: Any
            Additional arguments passed to the underlying filesystem method.
        """
        import fsspec

        path = _clean_path(path)
        this_fs = fsspec.filesystem(
            self.fsspec_protocol,
            connection_string=self.connection_string,
            asyncronous=True,
        )
        return await this_fs._rm(
            path=path,
            recursive=recursive,
            maxdepth=maxdepth,
            delimiter=delimiter,
            expand_path=expand_path,
            **kwargs,
        )

    def make_sas_link(
        self,
        filepath: str,
        expiry: datetime | None = None,
        *,
        write: bool = False,
        content_disposition_filename: str | None = None,
    ) -> str:
        """
        Create a shareable direct link with a SAS token.

        Parameters
        ----------
        filepath: str
            Path to the blob.
        expiry: datetime | None, default=None
            Expiration time for the link. Defaults to 2050-01-01 for read-only
            links and 30 minutes from now for writable links.
        write: bool, default=False
            Whether the link permits writing.
        content_disposition_filename: str | None, default=None
            File name for the Content-Disposition header.

        Returns
        -------
        str
            Direct blob URL with a SAS token attached.
        """
        import azure.storage.blob as asb

        filepath = _clean_path(filepath)
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
        container_name, blob_name = filepath.split("/", 1)
        sas = asb.generate_blob_sas(
            account_name=account_dict["AccountName"],
            account_key=account_dict["AccountKey"],
            container_name=container_name,
            blob_name=blob_name,
            permission=asb.BlobSasPermissions(read=True, write=write),
            expiry=expiry,
            content_disposition=content_disposition,
        )
        return f"https://{account_dict['AccountName']}.blob.core.windows.net/{filepath}?{sas}"

    async def stream_read(self, path: str) -> AsyncGenerator[bytes]:
        """
        Yield the contents of a remote blob in chunks.

        Parameters
        ----------
        path: str
            The remote blob path to read.

        Yields
        ------
        bytes
            The next chunk of blob content.
        """
        from azure.storage.blob.aio import BlobClient

        path = _clean_path(path)
        async with BlobClient.from_connection_string(
            self.connection_string, *(path.split("/", maxsplit=1))
        ) as blob:
            stream = await blob.download_blob()

            async for chunk in stream.chunks():
                yield chunk

    async def read(self, path: str) -> bytes:
        """
        Read the complete contents of a remote blob.

        Parameters
        ----------
        path: str
            The remote blob path to read.

        Returns
        -------
        bytes
            The blob contents.
        """
        from azure.storage.blob.aio import BlobClient

        path = _clean_path(path)
        async with BlobClient.from_connection_string(
            self.connection_string, *(path.split("/", maxsplit=1))
        ) as blob:
            stream = await blob.download_blob()
            return await stream.read()

    async def lock(self, lock_path: str, timeout_sec: int = 0) -> Lock:
        """
        Create an asynchronous distributed lock for a blob path.

        Parameters
        ----------
        lock_path: str
            The blob path used to hold the lock.
        timeout_sec: int, default=0
            Maximum time to wait for the lock. A value of zero means no wait.

        Returns
        -------
        Lock
            An asynchronous lock context manager.
        """
        return Lock(self.connection_string, lock_path, timeout_sec)

    def writer(self, path: str) -> abfs_writer:
        """
        Create an asynchronous block-blob writer.

        Parameters
        ----------
        path: str
            The remote blob path to write.

        Returns
        -------
        abfs_writer
            An asynchronous writer context manager.
        """
        path = _clean_path(path)
        return abfs_writer(self.connection_string, path)

    async def read_json(self, path: str) -> dict | list:
        """
        Read and deserialize JSON data from a remote blob.

        Parameters
        ----------
        path: str
            The remote blob path containing JSON data.

        Returns
        -------
        dict | list
            The decoded JSON value.
        """
        try:
            import orjson

            loads = orjson.loads
        except ModuleNotFoundError:
            import json

            loads = json.loads
        path = _clean_path(path)
        data = await self.read(path)
        if _looks_like_gzip(data):
            from gzip import GzipFile
            from io import BytesIO

            with GzipFile(fileobj=BytesIO(data)) as f:
                data = f.read()

        return loads(data)


def _looks_like_gzip(data: bytes) -> bool:
    return (
        len(data) >= 10
        and data[0:2] == b"\x1f\x8b"  # gzip magic number
        and data[2] == 8  # DEFLATE
        and (data[3] & 0xE0) == 0  # reserved flags must be zero
    )


async def _stage_block(target: BlobClient, block_id: str, chunk: bytes):
    return await target.stage_block(block_id=block_id, data=cast("IO", chunk))


def _block_task(
    tasks: set[asyncio.Task],
    target: BlobClient,
    block_list: list[BlobBlock],
    chunk: bytes,
):
    from azure.storage.blob import BlobBlock

    block_id = uuid4().hex
    tasks.add(asyncio.create_task(_stage_block(target, block_id, chunk)))
    block_list.append(BlobBlock(block_id=block_id))


class MinMaxNotEqualError(Exception):
    pass


class ColumnNotExistError(Exception):
    pass


class Lock:
    def __init__(self, conn_str: str, lock_path: str, timeout_sec: int = 0):
        self.lock_path = lock_path
        self.conn_str = conn_str
        self.keep_renewing = True
        self.timeout = timeout_sec

    async def renewer(self):
        """Renew the blob lease until lock shutdown or a service error."""
        from azure.core.exceptions import HttpResponseError

        while self.keep_renewing:
            try:
                if self.lease:
                    await self.lease.renew()
            except HttpResponseError:
                break
            await asyncio.sleep(30)

    async def __aenter__(self):
        import time

        from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
        from azure.storage.blob.aio import BlobClient

        container_name, blob_name = self.lock_path.split("/", maxsplit=1)
        self.blob_client = BlobClient.from_connection_string(
            self.conn_str,
            container_name=container_name,
            blob_name=blob_name,
        )
        strt = time.time()
        first_attempt = True
        self.lease = None
        while True:
            try:
                self.lease = await self.blob_client.acquire_lease(60)
                break
            except ResourceNotFoundError:
                if first_attempt:
                    await self.blob_client.upload_blob(b"", overwrite=True)
                    self.lease = await self.blob_client.acquire_lease(60)
                    first_attempt = False
                else:
                    raise
            except ResourceExistsError:
                await asyncio.sleep(0.5)
            if time.time() - strt > self.timeout:
                msg = "Could not acquire lock within timeout"
                raise TimeoutError(msg)
        assert self.lease is not None
        self.renew_task = asyncio.create_task(self.renewer())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.keep_renewing = False
        self.renew_task.cancel()
        if self.lease:
            await self.lease.release()

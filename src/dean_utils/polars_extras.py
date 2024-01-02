import polars as pl
from polars.type_aliases import (
    ParallelStrategy,
    ParquetCompression,
    ColumnNameOrSelector,
)
import pyarrow.parquet as pq
import fsspec
import os
from typing import Any, Sequence

abfs = fsspec.filesystem("abfss", connection_string=os.environ["Synblob"])

key_conv = {"AccountName": "account_name", "AccountKey": "account_key"}
stor = {(splt := x.split("=", 1))[0]: splt[1] for x in os.environ["Synblob"].split(";")}
stor = {key_conv[key]: val for key, val in stor.items() if key in key_conv.keys()}


def pl_scan_pq(
    source: str,
    *,
    n_rows: int | None = None,
    row_count_name: str | None = None,
    row_count_offset: int = 0,
    parallel: ParallelStrategy = "auto",
    use_statistics: bool = True,
    rechunk: bool = True,
    low_memory: bool = False,
    cache: bool = True,
    storage_options=None,
    retries: int = 0,
) -> pl.LazyFrame:
    """
    # wrapper for pl.scan_parquet that prepends abfs:// to the path, injects user credentials from Synblob env variable, and sets hive to False

    Parameters
    ----------
    source
        Path(s) to a file
        If a single path is given, it can be a globbing pattern.
    n_rows
        Stop reading from parquet file after reading `n_rows`.
    row_count_name
        If not None, this will insert a row count column with the given name into the
        DataFrame
    row_count_offset
        Offset to start the row_count column (only used if the name is set)
    parallel : {'auto', 'columns', 'row_groups', 'none'}
        This determines the direction of parallelism. 'auto' will try to determine the
        optimal direction.
    use_statistics
        Use statistics in the parquet to determine if pages
        can be skipped from reading.
    rechunk
        In case of reading multiple files via a glob pattern rechunk the final DataFrame
        into contiguous memory chunks.
    low_memory
        Reduce memory pressure at the expense of performance.
    cache
        Cache the result after reading.
    retries
        Number of retries if accessing a cloud instance fails."""
    if storage_options is None:
        storage_options = stor
    return pl.scan_parquet(
        f"abfs://{source}",
        n_rows=n_rows,
        cache=cache,
        parallel=parallel,
        rechunk=rechunk,
        row_count_name=row_count_name,
        row_count_offset=row_count_offset,
        low_memory=low_memory,
        use_statistics=use_statistics,
        retries=retries,
        storage_options=storage_options,
        hive_partitioning=False,
    )


def pl_scan_hive(
    source: str,
    *,
    n_rows: int | None = None,
    row_count_name: str | None = None,
    row_count_offset: int = 0,
    parallel: ParallelStrategy = "auto",
    use_statistics: bool = True,
    rechunk: bool = True,
    low_memory: bool = False,
    cache: bool = True,
    storage_options=None,
    retries: int = 0,
) -> pl.LazyFrame:
    """
    # wrapper for pl.scan_parquet that prepends abfs:// to the path, injects user credentials from Synblob env variable, and sets hive to False

    Parameters
    ----------
    source
        Path(s) to a file
        If a single path is given, it can be a globbing pattern.
    n_rows
        Stop reading from parquet file after reading `n_rows`.
    row_count_name
        If not None, this will insert a row count column with the given name into the
        DataFrame
    row_count_offset
        Offset to start the row_count column (only used if the name is set)
    parallel : {'auto', 'columns', 'row_groups', 'none'}
        This determines the direction of parallelism. 'auto' will try to determine the
        optimal direction.
    use_statistics
        Use statistics in the parquet to determine if pages
        can be skipped from reading.
    rechunk
        In case of reading multiple files via a glob pattern rechunk the final DataFrame
        into contiguous memory chunks.
    low_memory
        Reduce memory pressure at the expense of performance.
    cache
        Cache the result after reading.
    retries
        Number of retries if accessing a cloud instance fails."""
    if storage_options is None:
        storage_options = stor
    return pl.scan_parquet(
        f"abfs://{source}",
        n_rows=n_rows,
        cache=cache,
        parallel=parallel,
        rechunk=rechunk,
        row_count_name=row_count_name,
        row_count_offset=row_count_offset,
        low_memory=low_memory,
        use_statistics=use_statistics,
        retries=retries,
        storage_options=storage_options,
        hive_partitioning=True,
    )


def pl_write_pq(
    self,
    file: str,
    *,
    filesystem=abfs,
    compression: ParquetCompression = "zstd",
    compression_level: int | None = None,
    row_group_part: ColumnNameOrSelector | Sequence[ColumnNameOrSelector] | None = None,
    pyarrow_extra_options: dict[str, Any] | None = None,
) -> None:
    """
    Write to Apache Parquet file with pyarrow writer.
    Defaults to writing to Azure cloud as defined by Synblob env variable.

    Parameters
    ----------
    file
        ABFS File path to which the result will be written.

    compression : {'lz4', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'zstd'}
        Choose "zstd" for good compression performance.
        Choose "lz4" for fast compression/decompression.
        Choose "snappy" for more backwards compatibility guarantees
        when you deal with older parquet readers.

    compression_level
        The level of compression to use. Higher compression means smaller files on
        disk.

        - "gzip" : min-level: 0, max-level: 10.
        - "brotli" : min-level: 0, max-level: 11.
        - "zstd" : min-level: 1, max-level: 22.

    row_group_part: use partition_by to create defined row groups

    pyarrow_extra_options:
        Extra options to feed to pyarrow
    """
    if pyarrow_extra_options is None:
        pyarrow_extra_options = {}
    pq_common_params = dict(
        filesystem=filesystem,
        compression=compression,
        compression_level=compression_level,
        **pyarrow_extra_options,
    )

    if row_group_part is None:
        pq.write_table(self.to_arrow(), file, **pq_common_params)
    else:
        writer_params = dict(schema=self.to_arrow().schema, **pq_common_params)
        with pq.ParquetWriter(file, **writer_params) as writer:
            for row_group in self.partition_by(row_group_part):
                writer.write_table(row_group.to_arrow())


setattr(pl, "scan_pq", pl_scan_pq)
setattr(pl, "scan_hive", pl_scan_hive)
DF = pl.DataFrame
setattr(DF, "write_pq", pl_write_pq)
setattr(pl, "DataFrame", DF)

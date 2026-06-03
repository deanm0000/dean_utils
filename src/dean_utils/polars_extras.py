from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Sequence

    import polars as pl
    from fsspec import AbstractFileSystem
    from polars.type_aliases import (
        ColumnNameOrSelector,
        ParquetCompression,
    )

try:
    import fsspec

    abfs: AbstractFileSystem | None = fsspec.filesystem(
        "abfss", connection_string=os.environ["Synblob"]
    )
except Exception:
    abfs = None

key_conv = {"AccountName": "account_name", "AccountKey": "account_key"}
if "Synblob" in os.environ:
    stor = {
        (splt := x.split("=", 1))[0]: splt[1] for x in os.environ["Synblob"].split(";")
    }
    stor = {key_conv[key]: val for key, val in stor.items() if key in key_conv}
else:
    stor = {}


def pl_write_pq(
    df: pl.DataFrame,
    file: str,
    *,
    filesystem: AbstractFileSystem | None = abfs,
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
    df: pl.DataFrame
        DataFrame to write out.
    file
        ABFS File path to which the result will be written.
    filesystem: fsspec AbstractFileSystem, optional
        The filesystem to use for writing. If None, it will default to an ABFS filesystem
    compression : {'lz4', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'zstd'}
        Choose "zstd" for good compression performance.
        Choose "lz4" for fast compression/decompression.
        Choose "snappy" for more backwards compatibility guarantees
        when you deal with older parquet readers.

    compression_level: int | None
        The level of compression to use. Higher compression means smaller files on
        disk.

        - "gzip" : min-level: 0, max-level: 10.
        - "brotli" : min-level: 0, max-level: 11.
        - "zstd" : min-level: 1, max-level: 22.

    row_group_part: ColumnNameOrSelector | Sequence[ColumnNameOrSelector] | None
        Use partition_by to create defined row groups.

    pyarrow_extra_options: dict[str, Any] | None
        Extra options to feed to pyarrow
    """
    import pyarrow.parquet as pq

    if filesystem is None:
        msg = "filesystem is required"
        raise ValueError(msg)
    if pyarrow_extra_options is None:
        pyarrow_extra_options = {}

    if row_group_part is None:
        pq.write_table(
            df.to_arrow(),
            file,
            filesystem=filesystem,
            compression=compression,
            compression_level=compression_level,
            **pyarrow_extra_options,
        )
    else:
        with pq.ParquetWriter(
            file,
            schema=df.to_arrow().schema,
            filesystem=filesystem,
            compression=compression,
            compression_level=compression_level,
            **pyarrow_extra_options,
        ) as writer:
            for row_group in df.partition_by(row_group_part):
                writer.write_table(row_group.to_arrow())


def unnest_all(df: pl.DataFrame):
    """
    Unnests all struct columns into individual columns with root.sub style naming.

    Args:
        df (pl.DataFrame): The DataFrame to unnest.

    Returns
    -------
        pl.DataFrame: A new DataFrame with all struct columns unnested.
    """
    from math import ceil, log10

    length = ceil(log10(1 + len(df.columns)))

    to_do = [
        (pl.col(x), y, x, str(i).zfill(length))
        for i, (x, y) in enumerate(df.schema.items())
    ]
    out_cols: list[tuple[pl.Expr, str]] = []
    while to_do:
        this_col, dtype, prefix, i = to_do.pop()
        if dtype != pl.Struct:
            out_cols.append((this_col.alias(prefix), i))
            continue

        dtype = cast(pl.Struct, dtype)
        jlength = ceil(log10(1 + len(dtype.fields)))
        for j, field in enumerate(dtype.fields):
            field_dtype = cast(pl.DataType, field.dtype)

            to_do.append(
                (
                    this_col.struct.field(field.name),
                    field_dtype,
                    f"{prefix}.{field.name}",
                    f"{i}{str(j).zfill(jlength)}",
                )
            )
    out_cols.sort(key=lambda x: x[1])
    return df.select(c[0] for c in out_cols)


# def pl_write_delta_append(
#     df: pl.DataFrame,
#     target: str | Path | DeltaTable,
#     *,
#     storage_options: dict[str, str] | None = None,
#     delta_write_options: dict[str, Any] | None = None,
# ):
#     """
#     Appends DataFrame to delta table and auto computes range partition.

#     Parameters
#     ----------
#         df
#             df to be saved
#         target
#             URI of a table or a DeltaTable object.
#         storage_options
#             Extra options for the storage backends supported by `deltalake`.
#             For cloud storages, this may include configurations for authentication etc.

#             - See a list of supported storage options for S3 `here <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants>`__.
#             - See a list of supported storage options for GCS `here <https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants>`__.
#             - See a list of supported storage options for Azure `here <https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants>`__.
#         delta_write_options
#             Additional keyword arguments while writing a Delta lake Table.
#             See a list of supported write options `here <https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.write_deltalake>`__.
#     """
#     from deltalake import DeltaTable, WriterProperties

#     if isinstance(target, (str, Path)):
#         target = DeltaTable(target, storage_options=storage_options)
#     add_actions = cast(pl.DataFrame, pl.from_arrow(target.get_add_actions()))
#     ### Only allows single column partition range
#     partition_by = (
#         add_actions.slice(1)
#         .select("partition_values")
#         .unnest("partition_values")
#         .columns[0]
#     )
#     partition_col = partition_by.replace("_range", "")
#     ranges = (
#         add_actions.select(
#             pl.col("partition_values").struct.field(partition_by),
#             pl.col("min")
#             .struct.field(partition_col)
#             .alias("min_id")
#             .cast(df.schema[partition_col]),
#             pl.col("max").struct.field(partition_col).alias("max_id"),
#         )
#         .group_by(partition_by)
#         .agg(pl.col("min_id").min(), pl.col("max_id").max())
#         .sort("min_id")
#     )
#     initial_height = df.height
#     df = df.sort(partition_col).join_asof(
#         ranges, left_on=partition_col, right_on="min_id"
#     )
#     assert df.height == initial_height
#     assert df.filter(pl.col(partition_col) < pl.col("min_id")).height == 0
#     df = df.drop("min_id", "max_id")
#     assert isinstance(target, DeltaTable)

#     if delta_write_options is None:
#         delta_write_options = {
#             "writer_properties": WriterProperties(compression="ZSTD"),
#             "engine": "rust",
#         }
#     else:
#         if "engine" not in delta_write_options:
#             delta_write_options["engine"] = "rust"
#         if "writer_properties" in delta_write_options:
#             if delta_write_options["writer_properties"].compression is None:
#                 delta_write_options["writer_properties"].compression = "ZSTD(1)"
#         else:
#             delta_write_options["writer_properties"] = WriterProperties(
#                 compression="ZSTD"
#             )
#     df.write_delta(
#         target=target,
#         mode="append",
#         storage_options=storage_options,
#         delta_write_options=delta_write_options,
#     )

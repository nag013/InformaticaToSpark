"""Cloud-agnostic Spark data reader utilities.

The module keeps provider-specific assumptions out of the read path. It works
with Spark tables when a metastore/catalog is available, and with paths when
data is addressed directly in object storage or local files.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

LOGGER = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ReaderError(Exception):
    """Raised when a read target cannot be validated or loaded."""


@dataclass(frozen=True)
class ReadTarget:
    """Description of data to read with Spark.

    Use either ``table`` or ``path``. Table reads are appropriate for Spark
    catalogs and metastores. Path reads are appropriate for S3, ABFS, GCS,
    DBFS, HDFS, and local files.
    """

    table: Optional[str] = None
    path: Optional[str] = None
    schema: Optional[str] = None
    catalog: Optional[str] = None
    fmt: str = "delta"
    options: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if bool(self.table) == bool(self.path):
            raise ReaderError("Provide exactly one read target: table or path.")
        if not self.fmt:
            raise ReaderError("Read format cannot be empty.")

    @property
    def description(self) -> str:
        if self.table:
            return build_fqn(self.table, self.schema, self.catalog)
        return f"{self.fmt}:{self.path}"


def get_spark_session(app_name: str = "ETL_Reader") -> Any:
    """Return an active Spark session, creating one when needed."""

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ReaderError("PySpark is required to create a SparkSession.") from exc

    return SparkSession.builder.appName(app_name).getOrCreate()


def build_fqn(
    table: str,
    schema: Optional[str] = None,
    catalog: Optional[str] = None,
) -> str:
    """Build a Spark table identifier from optional catalog and schema parts."""

    parts = [part for part in (catalog, schema, table) if part]
    if not parts:
        raise ReaderError("Table name cannot be empty.")

    for part in parts:
        if not _IDENTIFIER_RE.match(part):
            raise ReaderError(
                f"Invalid Spark identifier '{part}'. Use simple unquoted identifiers."
            )

    return ".".join(parts)


def validate_table_exists(spark: Any, table_fqn: str) -> None:
    """Ensure a Spark table is visible to the provided session."""

    try:
        exists = spark.catalog.tableExists(table_fqn)
    except Exception as exc:
        raise ReaderError(f"Failed to check table accessibility: {table_fqn}") from exc

    if not exists:
        raise ReaderError(f"Table does not exist or is not accessible: {table_fqn}")


def _load_path(spark: Any, target: ReadTarget) -> Any:
    reader = spark.read.format(target.fmt)
    for key, value in target.options.items():
        reader = reader.option(key, value)
    return reader.load(target.path)


def read_data(
    target: ReadTarget,
    spark: Optional[Any] = None,
    validate: bool = True,
) -> Any:
    """Read a Spark table or path.

    The function avoids eager actions such as ``count()`` so callers do not
    accidentally trigger expensive jobs during pipeline planning.
    """

    spark = spark or get_spark_session()
    LOGGER.info("Reading data from %s", target.description)

    try:
        if target.table:
            table_fqn = build_fqn(target.table, target.schema, target.catalog)
            if validate:
                validate_table_exists(spark, table_fqn)
            return spark.table(table_fqn)

        return _load_path(spark, target)
    except ReaderError:
        raise
    except Exception as exc:
        raise ReaderError(f"Failed to read {target.description}: {exc}") from exc


def read_table(
    table: str,
    schema: Optional[str] = None,
    catalog: Optional[str] = None,
    spark: Optional[Any] = None,
    validate: bool = True,
) -> Any:
    """Read a Spark table from any catalog/metastore supported by Spark."""

    target = ReadTarget(table=table, schema=schema, catalog=catalog)
    return read_data(target, spark=spark, validate=validate)


def read_path(
    path: str,
    fmt: str = "delta",
    options: Optional[Mapping[str, str]] = None,
    spark: Optional[Any] = None,
) -> Any:
    """Read files by path using a Spark format and reader options."""

    target = ReadTarget(path=path, fmt=fmt, options=options or {})
    return read_data(target, spark=spark, validate=False)


def _filter_incremental(df: Any, increment_column: str, end_value: str) -> Any:
    if increment_column not in df.columns:
        raise ReaderError(f"Increment column '{increment_column}' not found.")

    try:
        from importlib import import_module

        functions = import_module("pyspark.sql.functions")
    except ImportError as exc:
        raise ReaderError("PySpark is required for incremental filtering.") from exc

    return df.filter(functions.col(increment_column) > functions.lit(end_value))


def read_incremental(
    target: ReadTarget,
    increment_column: str,
    end_value: Optional[str],
    spark: Optional[Any] = None,
    validate: bool = True,
) -> Any:
    """Read a target and apply an increment filter when an end value is supplied."""

    if not increment_column:
        raise ReaderError("Increment column cannot be empty.")

    df = read_data(target, spark=spark, validate=validate)
    if end_value is None:
        LOGGER.info("No end value provided for %s; returning full load.", target.description)
        return df

    LOGGER.info(
        "Applying incremental filter on %s using increment column %s",
        target.description,
        increment_column,
    )
    return _filter_incremental(df, increment_column, end_value)


def read_table_incremental(
    table: str,
    increment_column: str,
    end_value: Optional[str],
    schema: Optional[str] = None,
    catalog: Optional[str] = None,
    spark: Optional[Any] = None,
    validate: bool = True,
) -> Any:
    """Read a table and filter rows newer than ``end_value``."""

    target = ReadTarget(table=table, schema=schema, catalog=catalog)
    return read_incremental(
        target,
        increment_column=increment_column,
        end_value=end_value,
        spark=spark,
        validate=validate,
    )


def read_path_incremental(
    path: str,
    increment_column: str,
    end_value: Optional[str],
    fmt: str = "delta",
    options: Optional[Mapping[str, str]] = None,
    spark: Optional[Any] = None,
) -> Any:
    """Read a path and filter rows newer than ``end_value``."""

    target = ReadTarget(path=path, fmt=fmt, options=options or {})
    return read_incremental(
        target,
        increment_column=increment_column,
        end_value=end_value,
        spark=spark,
        validate=False,
    )

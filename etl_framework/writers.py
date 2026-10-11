"""Cloud-agnostic Spark DataFrame writer utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

LOGGER = logging.getLogger(__name__)


class WriterError(Exception):
    """Raised when a DataFrame cannot be written."""


@dataclass(frozen=True)
class WriteTarget:
    """Description of a write target."""

    table: Optional[str] = None
    path: Optional[str] = None
    fmt: str = "delta"
    mode: str = "append"
    partition_by: tuple[str, ...] = ()
    options: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if bool(self.table) == bool(self.path):
            raise WriterError("Provide exactly one write target: table or path.")


def write_data(df: Any, target: WriteTarget) -> None:
    """Write a DataFrame to a table or path."""

    writer = df.write.format(target.fmt).mode(target.mode)
    for key, value in target.options.items():
        writer = writer.option(key, value)
    if target.partition_by:
        writer = writer.partitionBy(*target.partition_by)

    destination = target.table or target.path
    LOGGER.info("Writing data to %s using mode=%s format=%s", destination, target.mode, target.fmt)

    try:
        if target.table:
            writer.saveAsTable(target.table)
        else:
            writer.save(target.path)
    except Exception as exc:
        raise WriterError(f"Failed writing to {destination}: {exc}") from exc


def append_table(df: Any, table: str, fmt: str = "delta") -> None:
    """Append a DataFrame to a Spark table."""

    write_data(df, WriteTarget(table=table, fmt=fmt, mode="append"))


def overwrite_path(df: Any, path: str, fmt: str = "delta") -> None:
    """Overwrite a filesystem or object-store path."""

    write_data(df, WriteTarget(path=path, fmt=fmt, mode="overwrite"))


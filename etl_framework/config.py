"""Configuration models and loaders for ETL pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional


class ConfigError(Exception):
    """Raised when pipeline configuration is invalid."""


@dataclass(frozen=True)
class DatasetRef:
    """Reference to either a Spark table or path target."""

    table: Optional[str] = None
    path: Optional[str] = None
    schema: Optional[str] = None
    catalog: Optional[str] = None
    fmt: str = "delta"
    options: Mapping[str, str] = field(default_factory=dict)

    def validate(self) -> None:
        if bool(self.table) == bool(self.path):
            raise ConfigError("DatasetRef requires exactly one of table or path.")


@dataclass(frozen=True)
class PipelineConfig:
    """Top-level pipeline configuration."""

    name: str
    source: DatasetRef
    target: DatasetRef
    load_type: str = "append"
    primary_keys: tuple[str, ...] = ()
    watermark_column: Optional[str] = None
    options: Mapping[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.name:
            raise ConfigError("Pipeline name is required.")
        self.source.validate()
        self.target.validate()
        if self.load_type not in {"append", "overwrite", "upsert", "scd2"}:
            raise ConfigError(f"Unsupported load_type: {self.load_type}")
        if self.load_type in {"upsert", "scd2"} and not self.primary_keys:
            raise ConfigError(f"{self.load_type} requires primary_keys.")


def _dataset_from_mapping(raw: Mapping[str, Any]) -> DatasetRef:
    return DatasetRef(
        table=raw.get("table"),
        path=raw.get("path"),
        schema=raw.get("schema"),
        catalog=raw.get("catalog"),
        fmt=raw.get("fmt", "delta"),
        options=raw.get("options", {}),
    )


def pipeline_from_mapping(raw: Mapping[str, Any]) -> PipelineConfig:
    """Create a validated ``PipelineConfig`` from a dict-like object."""

    config = PipelineConfig(
        name=raw["name"],
        source=_dataset_from_mapping(raw["source"]),
        target=_dataset_from_mapping(raw["target"]),
        load_type=raw.get("load_type", "append"),
        primary_keys=tuple(raw.get("primary_keys", ())),
        watermark_column=raw.get("watermark_column"),
        options=raw.get("options", {}),
    )
    config.validate()
    return config


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    """Load JSON pipeline configuration from disk."""

    with Path(path).open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    return pipeline_from_mapping(raw)


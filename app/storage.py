"""CSV parsing and crash-safe file operations on the shared volume."""
from __future__ import annotations

import csv
import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from . import config


class CsvValidationError(ValueError):
    """Raised when an uploaded CSV does not satisfy the ingestion contract."""


@dataclass(frozen=True)
class ParsedCsv:
    rows: list[tuple[str, str, str]]  # (subject_id, sample_id, value)


def _fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` atomically (tmp + fsync + rename + dir fsync)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def parse_upload(data: bytes) -> ParsedCsv:
    """Validate an uploaded CSV: exact header, <= 1000 data rows, 3 columns."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("CSV must be UTF-8 encoded") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise CsvValidationError("empty CSV file") from exc

    normalized = [h.strip().lower() for h in header]
    if normalized != ["subject_id", "sample_id", "value"]:
        raise CsvValidationError(
            "CSV header must be exactly: subject_id,sample_id,value"
        )

    rows: list[tuple[str, str, str]] = []
    seen_sample_ids: set[str] = set()
    for line_no, record in enumerate(reader, start=2):
        if not record or all(cell == "" for cell in record):
            continue  # tolerate blank trailing lines
        if len(record) != 3:
            raise CsvValidationError(
                f"line {line_no}: expected 3 columns, got {len(record)}"
            )
        subject_id, sample_id, value = (c.strip() for c in record)
        if not subject_id or not sample_id:
            raise CsvValidationError(
                f"line {line_no}: subject_id and sample_id must not be empty"
            )
        if sample_id in seen_sample_ids:
            raise CsvValidationError(
                f"line {line_no}: duplicate sample_id {sample_id!r} inside file"
            )
        seen_sample_ids.add(sample_id)
        rows.append((subject_id, sample_id, value))
        if len(rows) > config.MAX_ROWS_PER_CSV:
            raise CsvValidationError(
                f"CSV exceeds the limit of {config.MAX_ROWS_PER_CSV} data rows"
            )

    if not rows:
        raise CsvValidationError("CSV contains no data rows")
    return ParsedCsv(rows=rows)


def render_csv(rows: Iterable[tuple[str, str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["subject_id", "sample_id", "value"])
    writer.writerows(rows)
    return buf.getvalue()


def shard_path(shard_id: int) -> Path:
    return config.SHARD_DIR / f"shard-{shard_id:04d}.csv"


def staging_path(shard_id: int, task_id: str) -> Path:
    safe = task_id.replace("/", "_")
    return config.STAGING_DIR / f"shard-{shard_id:04d}.{safe}.tmp.csv"


def source_path(source_file: str) -> Path:
    # source_file is generated server-side, never user supplied; guard anyway.
    if "/" in source_file or ".." in source_file:
        raise ValueError("illegal source file name")
    return config.INGEST_DIR / source_file


def read_source_rows(source_file: str) -> list[tuple[str, str, str]]:
    """Read a stored source CSV, returning data rows in file order."""
    with open(source_path(source_file), newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        if [h.strip().lower() for h in header] != [
            "subject_id",
            "sample_id",
            "value",
        ]:
            raise ValueError(f"{source_file}: unexpected header")
        return [(r[0], r[1], r[2]) for r in reader if r]


def swap_staged_shard(shard_id: int, staged: Path, target: Path) -> None:
    """Publish a purified shard.

    Contract from the spec: before completion, remove the old downloadable
    file and any stale staging files, and only then make the purified file
    visible. The shard has already been marked WITHHELD in the database, so
    new downloads are rejected regardless of file presence.
    """
    if target.exists():
        target.unlink()
    for stale in config.STAGING_DIR.glob(f"shard-{shard_id:04d}.*.tmp.csv"):
        if stale != staged:
            stale.unlink()
    _fsync_dir(config.SHARD_DIR)
    os.replace(staged, target)
    _fsync_dir(config.SHARD_DIR)
    _fsync_dir(config.STAGING_DIR)


def rewrite_source_without(source_file: str, subject_id: str) -> int:
    """Rewrite an ingestion CSV dropping one subject's rows.

    Returns the number of rows removed. Crash-safe via atomic write: the old
    file stays valid until the new one is fully on disk.
    """
    path = source_path(source_file)
    kept: list[tuple[str, str, str]] = []
    removed = 0
    for row in read_source_rows(source_file):
        if row[0] == subject_id:
            removed += 1
        else:
            kept.append(row)
    atomic_write(path, render_csv(kept))
    return removed

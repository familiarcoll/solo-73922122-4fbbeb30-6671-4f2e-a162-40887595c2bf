"""One-shot ingestion: at most three CSV files, never appended to later."""
from __future__ import annotations

import math
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import config, storage
from .storage import CsvValidationError, ParsedCsv, atomic_write, parse_upload, render_csv, shard_path


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class AlreadyIngestedError(RuntimeError):
    pass


def is_ingested(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM meta WHERE key='ingested'").fetchone()
    return row is not None and row["value"] == "1"


def ingest(conn: sqlite3.Connection, files: list[bytes]) -> dict:
    """Create the library from up to three uploaded CSVs.

    Validation happens before anything is written. The database is updated in
    a single serialised transaction so ingestion is all-or-nothing.
    """
    if not files:
        raise CsvValidationError("at least one CSV file is required")
    if len(files) > config.MAX_UPLOAD_FILES:
        raise CsvValidationError(
            f"at most {config.MAX_UPLOAD_FILES} CSV files may be uploaded"
        )

    parsed: list[ParsedCsv] = [parse_upload(data) for data in files]

    # Cross-file uniqueness check.
    seen: set[str] = set()
    total_rows = 0
    for parsed_file in parsed:
        total_rows += len(parsed_file.rows)
        for _, sample_id, _ in parsed_file.rows:
            if sample_id in seen:
                raise CsvValidationError(
                    f"sample_id {sample_id!r} appears in more than one file"
                )
            seen.add(sample_id)

    target = config.SHARD_TARGET_ROWS
    shard_count = min(config.MAX_SHARDS, max(1, math.ceil(total_rows / target)))
    size = math.ceil(total_rows / shard_count)

    now = utcnow()
    written_sources: list[Path] = []
    written_shards: list[Path] = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        if is_ingested(conn):
            conn.execute("ROLLBACK")
            raise AlreadyIngestedError("the library has already been created")

        source_files: list[str] = []
        for idx, parsed_file in enumerate(parsed):
            source_file = f"source-{idx + 1:02d}.csv"
            atomic_write(
                storage.source_path(source_file),
                render_csv(parsed_file.rows),
            )
            written_sources.append(storage.source_path(source_file))
            source_files.append(source_file)

        # Build shard files from the validated rows in global order.
        shard_meta: list[dict] = []
        offset = 0
        all_rows: list[tuple[str, str, str]] = [
            row for parsed_file in parsed for row in parsed_file.rows
        ]
        for shard_id in range(shard_count):
            chunk = all_rows[offset : offset + size]
            offset += size
            path = shard_path(shard_id)
            atomic_write(path, render_csv(chunk))
            written_shards.append(path)
            shard_meta.append(
                {
                    "shard_id": shard_id,
                    "total_rows": len(chunk),
                    "current_file": path.name,
                }
            )

        sample_rows: list[tuple[str, str, int, str, int]] = []
        global_index = 0
        for parsed_file, source_file in zip(parsed, source_files):
            for source_row_no, (subject_id, sample_id, _value) in enumerate(
                parsed_file.rows
            ):
                shard_id = min(global_index // size, shard_count - 1)
                sample_rows.append(
                    (subject_id, sample_id, shard_id, source_file, source_row_no)
                )
                global_index += 1

        conn.executemany(
            """
            INSERT INTO samples
                (subject_id, sample_id, shard_id, source_file, source_row_no)
            VALUES (?, ?, ?, ?, ?)
            """,
            sample_rows,
        )
        conn.executemany(
            """
            INSERT INTO shards (shard_id, status, total_rows, current_file, updated_at)
            VALUES (:shard_id, 'AVAILABLE', :total_rows, :current_file, :now)
            """,
            [{**m, "now": now} for m in shard_meta],
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('ingested', '1')"
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("ingested_at", now),
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("ingestion_id", uuid.uuid4().hex),
        )
        conn.execute("COMMIT")
    except AlreadyIngestedError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        for path in written_sources + written_shards:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        raise

    return {
        "status": "ingested",
        "files_ingested": len(parsed),
        "total_rows": total_rows,
        "shard_count": shard_count,
        "shards": shard_meta,
    }

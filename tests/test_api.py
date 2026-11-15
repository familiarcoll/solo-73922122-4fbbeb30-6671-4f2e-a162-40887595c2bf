"""End-to-end functional tests against a real uvicorn process."""
from __future__ import annotations

import time
from pathlib import Path

import httpx
import pytest

from conftest import make_csv, read_csv_rows, wait_task


def upload(client: httpx.Client, *payloads: bytes) -> httpx.Response:
    files = [("files", (f"f{i}.csv", p, "text/csv")) for i, p in enumerate(payloads)]
    return client.post("/ingest", files=files)


def test_health_and_docs(server):
    c = server.client()
    assert c.get("/healthz").json()["status"] == "ok"
    assert c.get("/docs").status_code == 200
    assert c.get("/openapi.json").status_code == 200
    # root redirects to the docs demo entry
    r = c.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"] == "/docs"


def test_ingest_limits_and_validation(server):
    c = server.client()

    # nothing ingested yet
    assert c.get("/shards").status_code == 409

    bad_header = b"a,b,c\n1,2,3\n"
    r = upload(c, bad_header)
    assert r.status_code == 400
    assert "header" in r.json()["detail"]

    empty = make_csv([])
    assert upload(c, empty).status_code == 400

    too_many = [make_csv([("s", f"x{i}", "v")]) for i in range(4)]
    assert upload(c, *too_many).status_code == 400

    over_rows = make_csv(
        [("s1", f"sample-{i}", f"v{i}") for i in range(1001)]
    )
    assert upload(c, over_rows).status_code == 400

    dup_in_file = make_csv([("s1", "k", "v1"), ("s2", "k", "v2")])
    assert upload(c, dup_in_file).status_code == 400

    dup_across = [
        make_csv([("s1", "shared-key", "v1")]),
        make_csv([("s2", "shared-key", "v2")]),
    ]
    assert upload(c, *dup_across).status_code == 400


def test_full_withdrawal_flow(server, tmp_data: Path):
    c = server.client()
    rows1 = [(f"s{i % 5}", f"a-{i}", f"val-{i}") for i in range(120)]
    rows2 = [(f"s{i % 4}", f"b-{i}", f"valb-{i}") for i in range(80)]
    r = upload(c, make_csv(rows1), make_csv(rows2))
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["total_rows"] == 200
    assert body["files_ingested"] == 2
    shard_count = body["shard_count"]
    assert shard_count >= 2

    # second ingestion is refused forever
    r2 = upload(c, make_csv([("s9", "new-1", "v")]))
    assert r2.status_code == 409

    # every shard downloadable, union of contents equals all rows
    shards = c.get("/shards").json()["shards"]
    assert all(s["status"] == "AVAILABLE" for s in shards)
    served: list[tuple[str, str, str]] = []
    for s in shards:
        d = c.get(f"/shards/{s['shard_id']}/download")
        assert d.status_code == 200
        assert d.headers["content-type"].startswith("text/csv")
        served.extend(read_csv_rows(d.content))
    assert sorted(served) == sorted(rows1 + rows2)

    # withdraw subject s1 (exists in both source files and several shards)
    target = "s1"
    expected_target_rows = [r_ for r_ in rows1 + rows2 if r_[0] == target]
    r = c.post("/withdrawals", params={"subject_id": target})
    assert r.status_code == 202
    created = r.json()
    assert created["status"] == "PENDING"
    assert created["affected_rows"] == len(expected_target_rows)
    assert created["affected_shards"] >= 1
    task_id = created["task_id"]

    # immediately after acceptance, affected shards stop new downloads
    for s in shards:
        st = c.get(f"/shards/{s['shard_id']}/status").json()
        if st["shard_id"] in {ws["shard_id"] for ws in
                              c.get(f"/withdrawals/{task_id}").json()["shards"]}:
            assert st["available"] is False
            d = c.get(f"/shards/{s['shard_id']}/download")
            assert d.status_code == 409
        else:
            # other shards keep serving
            assert c.get(f"/shards/{s['shard_id']}/download").status_code == 200

    final = wait_task(c, task_id)
    assert final["purged_rows"] == len(expected_target_rows)
    assert final["source_purged"] == 2
    assert all(ws["state"] == "DONE" for ws in final["shards"])

    # all shards back online; target subject gone everywhere, others intact
    remaining: list[tuple[str, str, str]] = []
    for s in c.get("/shards").json()["shards"]:
        assert s["status"] == "AVAILABLE"
        d = c.get(f"/shards/{s['shard_id']}/download")
        assert d.status_code == 200
        remaining.extend(read_csv_rows(d.content))
    assert all(row[0] != target for row in remaining)
    assert sorted(remaining) == sorted(
        r_ for r_ in rows1 + rows2 if r_[0] != target
    )

    # on-disk source CSVs no longer contain the subject either
    sources = sorted((tmp_data / "ingest").glob("source-*.csv"))
    source_rows = []
    for p in sources:
        source_rows.extend(read_csv_rows(p.read_bytes()))
    assert all(row[0] != target for row in source_rows)
    assert len(source_rows) == 200 - len(expected_target_rows)

    # staging area is clean
    assert list((tmp_data / "shards" / "staging").iterdir()) == []


def test_idempotent_repeat_withdrawal(server):
    c = server.client()
    rows = [(f"s{i}", f"k-{i}", f"v{i}") for i in range(30)]
    assert upload(c, make_csv(rows)).status_code == 201

    r1 = c.post("/withdrawals", params={"subject_id": "s1"})
    assert r1.status_code == 202
    task1 = r1.json()["task_id"]
    wait_task(c, task1)

    r2 = c.post("/withdrawals", params={"subject_id": "s1"})
    assert r2.status_code == 202
    assert r2.json()["task_id"] == task1
    assert r2.json()["reused"] is True

    # unknown task -> 404
    assert c.get("/withdrawals/wdl-doesnotexist").status_code == 404


def test_withdraw_unknown_subject_completes_empty(server):
    c = server.client()
    assert upload(c, make_csv([("s1", "k1", "v1")])).status_code == 201
    r = c.post("/withdrawals", params={"subject_id": "ghost"})
    assert r.status_code == 202
    body = r.json()
    assert body["affected_rows"] == 0
    task = wait_task(c, body["task_id"])
    assert task["status"] == "COMPLETED"
    # shard stays available
    assert c.get("/shards/0/download").status_code == 200


def test_validation_blank_subject(server):
    c = server.client()
    upload(c, make_csv([("s1", "k1", "v1")]))
    assert c.post("/withdrawals", params={"subject_id": "  "}).status_code == 400
    assert c.get("/shards/99/status").status_code == 404


def test_boundary_exactly_1000_rows_and_three_files(server):
    c = server.client()
    exactly = make_csv([("s1", f"k{i}", "v") for i in range(1000)])
    r = upload(c, exactly)
    assert r.status_code == 201, r.text
    assert r.json()["total_rows"] == 1000

    # library now sealed: more files are refused (unique ids so only sealing fails)
    more = [make_csv([("s2", f"m-{j}", "v")]) for j in range(3)]
    assert upload(c, *more).status_code == 409


def test_rewrite_only_deletes_target_rows(server):
    """Purification must never touch rows of other subjects (order preserved)."""
    c = server.client()
    rows = [
        ("s1", "k1", "a"),
        ("s2", "k2", "b"),
        ("s1", "k3", "c"),
        ("s2", "k4", "d"),
        ("s3", "k5", "e"),
    ]
    assert upload(c, make_csv(rows)).status_code == 201
    task = c.post("/withdrawals", params={"subject_id": "s1"}).json()["task_id"]
    wait_task(c, task)
    served = read_csv_rows(c.get("/shards/0/download").content)
    assert served == [
        ("s2", "k2", "b"),
        ("s2", "k4", "d"),
        ("s3", "k5", "e"),
    ]

"""Two real uvicorn processes sharing one DATA_DIR (double-instance tests)."""
from __future__ import annotations

import threading

import httpx

from conftest import Server, make_csv, read_csv_rows, wait_task, free_port


def _start_pair(tmp_data, env_extra=None):
    a = Server(tmp_data, free_port(), env_extra=env_extra)
    b = Server(tmp_data, free_port(), env_extra=env_extra)
    a.wait_ready()
    b.wait_ready()
    return a, b


def test_concurrent_withdrawals_different_subjects(tmp_data):
    a, b = _start_pair(tmp_data)
    try:
        rows = [(f"s{i % 6}", f"k-{i}", f"v{i}") for i in range(240)]
        with a.client() as c:
            r = c.post(
                "/ingest",
                files=[("files", ("f.csv", make_csv(rows), "text/csv"))],
            )
            assert r.status_code == 201
            shard_count = r.json()["shard_count"]
        assert shard_count >= 2

        results = {}

        def withdraw(port: int, subject: str) -> None:
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=15)
            try:
                r = client.post("/withdrawals", params={"subject_id": subject})
                assert r.status_code == 202
                results[subject] = r.json()["task_id"]
                wait_task(client, results[subject])
            finally:
                client.close()

        t1 = threading.Thread(target=withdraw, args=(a.port, "s0"))
        t2 = threading.Thread(target=withdraw, args=(b.port, "s1"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # both instances see both tasks
        with a.client() as ca, b.client() as cb:
            for task_id in results.values():
                assert ca.get(f"/withdrawals/{task_id}").json()["status"] == "COMPLETED"
                assert cb.get(f"/withdrawals/{task_id}").json()["status"] == "COMPLETED"

            remaining = []
            for shard_id in range(shard_count):
                # every shard is available on at least one instance and identical
                da = ca.get(f"/shards/{shard_id}/download")
                db = cb.get(f"/shards/{shard_id}/download")
                assert da.status_code == db.status_code == 200
                assert da.content == db.content
                remaining.extend(read_csv_rows(da.content))

        subjects_left = {row[0] for row in remaining}
        assert "s0" not in subjects_left
        assert "s1" not in subjects_left
        # nobody else was removed, and the two withdrawn sets are disjoint rows
        expected = [r for r in rows if r[0] not in ("s0", "s1")]
        assert sorted(remaining) == sorted(expected)

        # source files also purified of both subjects on the shared volume
        for p in (tmp_data / "ingest").glob("source-*.csv"):
            source_rows = read_csv_rows(p.read_bytes())
            assert all(r[0] not in ("s0", "s1") for r in source_rows)
    finally:
        a.stop()
        b.stop()


def test_second_withdrawal_does_not_resurrect_first(tmp_data):
    """After s0 is purged, withdrawing s2 must not bring s0 rows back."""
    a, b = _start_pair(tmp_data)
    try:
        rows = [(f"s{i % 4}", f"k-{i}", f"v{i}") for i in range(120)]
        with a.client() as c:
            c.post("/ingest", files=[("files", ("f.csv", make_csv(rows), "text/csv"))])
            task0 = c.post("/withdrawals", params={"subject_id": "s0"}).json()["task_id"]
            wait_task(c, task0)

        with b.client() as c:
            task2 = c.post("/withdrawals", params={"subject_id": "s2"}).json()["task_id"]
            wait_task(c, task2)

        with a.client() as c:
            shard_count = c.get("/shards").json()["shards"]
            remaining = []
            for s in shard_count:
                remaining.extend(
                    read_csv_rows(c.get(f"/shards/{s['shard_id']}/download").content)
                )
        assert {r[0] for r in remaining} == {"s1", "s3"}
        assert sorted(remaining) == sorted(
            r for r in rows if r[0] in ("s1", "s3")
        )
    finally:
        a.stop()
        b.stop()


def test_ingestion_contention_single_winner(tmp_data):
    """Two instances cannot both create the library."""
    a, b = _start_pair(tmp_data)
    try:
        payload = make_csv([("s1", "k1", "v1")])
        barrier = threading.Barrier(2)

        def post(port, out):
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=15)
            barrier.wait()
            try:
                r = client.post(
                    "/ingest", files=[("files", ("f.csv", payload, "text/csv"))]
                )
                out.append(r.status_code)
            finally:
                client.close()

        codes: list[int] = []
        t1 = threading.Thread(target=post, args=(a.port, codes))
        t2 = threading.Thread(target=post, args=(b.port, codes))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert sorted(codes) == [201, 409]
    finally:
        a.stop()
        b.stop()

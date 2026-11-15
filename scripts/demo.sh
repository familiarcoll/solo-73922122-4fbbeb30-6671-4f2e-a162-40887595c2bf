#!/usr/bin/env bash
# End-to-end demo against a running instance (default: instance A on :8001).
# Usage: ./scripts/demo.sh [base_url]
set -euo pipefail

BASE="${1:-http://localhost:8001}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

say() { printf '\n=== %s ===\n' "$*"; }
jq_or_cat() { command -v jq >/dev/null && jq . || cat; }

say "health"
curl -fsS "$BASE/healthz" | jq_or_cat

say "build two CSV files (200 rows total, subjects s0..s4)"
python3 - "$TMP" <<'PY'
import csv, os, sys
out = sys.argv[1]
for name, prefix, n in (("one.csv", "a", 120), ("two.csv", "b", 80)):
    with open(os.path.join(out, name), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["subject_id", "sample_id", "value"])
        for i in range(n):
            w.writerow([f"s{i % 5}", f"{prefix}-{i}", f"val-{i}"])
PY
wc -l "$TMP"/*.csv

say "one-shot ingestion (second upload is rejected afterwards)"
curl -fsS -X POST "$BASE/ingest" \
    -F "files=@$TMP/one.csv;type=text/csv" \
    -F "files=@$TMP/two.csv;type=text/csv" | jq_or_cat

say "shard list"
curl -fsS "$BASE/shards" | jq_or_cat

say "withdraw subject s1 -> task id"
TASK=$(curl -fsS -X POST "$BASE/withdrawals?subject_id=s1" | tee "$TMP/task.json" | \
    python3 -c 'import json,sys; print(json.load(sys.stdin)["task_id"])')
cat "$TMP/task.json" | jq_or_cat
echo "task id: $TASK"

say "repeat withdrawal reuses the same task"
curl -fsS -X POST "$BASE/withdrawals?subject_id=s1" | jq_or_cat

say "poll task until COMPLETED"
for _ in $(seq 1 30); do
    BODY=$(curl -fsS "$BASE/withdrawals/$TASK")
    STATUS=$(echo "$BODY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
    echo "status=$STATUS"
    [ "$STATUS" = "COMPLETED" ] && { echo "$BODY" | jq_or_cat; break; }
    sleep 0.5
done

say "download every shard and prove s1 rows are gone"
python3 - "$BASE" <<'PY'
import csv, io, json, sys, urllib.request
base = sys.argv[1]
shards = json.load(urllib.request.urlopen(base + "/shards"))["shards"]
rows, withheld = [], 0
for s in shards:
    try:
        data = urllib.request.urlopen(
            urllib.request.Request(base + f"/shards/{s['shard_id']}/download")
        ).read()
    except urllib.error.HTTPError:
        withheld += 1
        continue
    r = csv.reader(io.StringIO(data.decode()))
    next(r)
    rows.extend(tuple(x) for x in r if x)
left = {row[0] for row in rows}
print("shards:", len(shards), "withheld now:", withheld)
print("subjects left:", sorted(left))
assert "s1" not in left
print(f"total rows served: {len(rows)} (expected 160 = 200 - 40)")
PY

say "Swagger UI: open $BASE/docs"

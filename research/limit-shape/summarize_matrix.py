#!/usr/bin/env python3
"""Tổng hợp log thô các ô đo thành bảng: ô | IP thoát | session | số 200 | Retry-After | thời điểm 429."""
import glob
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs")

rows = []
for path in sorted(glob.glob(os.path.join(LOGS, "limitshape_*.jsonl"))):
    cell = os.path.basename(path)[len("limitshape_"):-len(".jsonl")]
    if cell == "run":
        continue
    ok = 0
    first429 = None
    n = 0
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        n += 1
        if d["status"] == 200:
            ok += 1
        elif first429 is None:
            first429 = d
    ipfile = os.path.join(LOGS, "limitshape_%s_ip.txt" % cell)
    ip_before = ip_after = "?"
    if os.path.exists(ipfile):
        for line in open(ipfile):
            if line.startswith("ip_before="):
                ip_before = line.split("=", 1)[1].split(" ")[0]
            if line.startswith("ip_after="):
                ip_after = line.split("=", 1)[1].split(" ")[0]
    rows.append({
        "cell": cell,
        "ip_before": ip_before,
        "ip_after": ip_after,
        "n": n,
        "ok200": ok,
        "ra": (first429 or {}).get("ra", ""),
        "t429": (first429 or {}).get("t", ""),
        "err": ((first429 or {}).get("err") or "")[:80],
    })

print("%-6s %-18s %-18s %6s %8s %-9s %-9s" % ("cell", "ip_before", "ip_after", "reqs", "ok200", "retry_after", "t_429"))
for r in rows:
    print("%-6s %-18s %-18s %6d %8d %-9s %-9s" % (r["cell"], r["ip_before"], r["ip_after"], r["n"], r["ok200"], r["ra"], r["t429"]))

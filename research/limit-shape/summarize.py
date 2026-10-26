#!/usr/bin/env python3
"""Tóm tắt JSONL cell: đếm 200, ghi lần 429 đầu, retry-after."""
import json
import sys

cell = sys.argv[1] if len(sys.argv) > 1 else "?"
ok = 0
first429 = None
codes = {}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    d = json.loads(line)
    st = d["status"]
    codes[st] = codes.get(st, 0) + 1
    if st == 200:
        ok += 1
    elif first429 is None:
        first429 = d
    print("  %-3d status=%s ra=%s t=%s text=%r" % (d["i"], st, d.get("ra"), d.get("t"), (d.get("text") or "")[:40]))
print("SUMMARY cell=%s ok200=%d codes=%s" % (cell, ok, codes))
if first429:
    print("FIRST_429 cell=%s i=%s ra=%s t=%s err=%s" % (cell, first429["i"], first429.get("ra"), first429.get("t"), (first429.get("err") or "")[:160]))

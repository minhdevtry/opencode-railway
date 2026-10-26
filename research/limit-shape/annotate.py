#!/usr/bin/env python3
"""Đọc JSONL từ stdin, thêm cell/i, ghi ra stdout (dùng cho run_cell.sh)."""
import json
import sys

cell = sys.argv[1] if len(sys.argv) > 1 else "?"
for i, line in enumerate(sys.stdin):
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except Exception:
        print(line, file=sys.stderr)
        continue
    d["cell"] = cell
    d["i"] = i
    print(json.dumps(d, ensure_ascii=False))

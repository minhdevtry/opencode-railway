#!/usr/bin/env python3
"""Đọc JSONL từ stdin, thêm cell/i/model/ses, ghi ra stdout (dùng cho run_cell.sh)."""
import json
import os
import sys

cell = sys.argv[1] if len(sys.argv) > 1 else "?"
model = os.environ.get("MODEL", "?")
ses = os.environ.get("SES", "new-each")
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
    d["model"] = d.get("model", model)
    d["ses"] = d.get("ses", ses)
    print(json.dumps(d, ensure_ascii=False))

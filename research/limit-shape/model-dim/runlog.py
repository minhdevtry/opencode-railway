#!/usr/bin/env python3
"""Ghi 1 dòng log cho mỗi request ra logs/run.log.

usage: runlog.py <cell> [jsonl_file]
  - không có file: đọc JSONL từ stdin
  - có file: đọc JSONL từ file (dùng sau khi 1 ô chạy xong)
In ra stdout dạng:
ts=<giờ> cell=<ô> model=<model> ses=<session> i=<số thứ tự> status=<mã> ra=<Retry-After>
"""
import json
import os
import sys

cell = sys.argv[1] if len(sys.argv) > 1 else "?"
path = sys.argv[2] if len(sys.argv) > 2 else None
model = os.environ.get("MODEL", "?")
ses = os.environ.get("SES", "new-each")

src = open(path) if path else sys.stdin
for line in src:
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except Exception:
        print(line, file=sys.stderr)
        continue
    print(
        "ts=%s cell=%s model=%s ses=%s i=%s status=%s ra=%s"
        % (d.get("t"), cell, d.get("model", model), d.get("ses", ses), d.get("i"), d.get("status"), d.get("ra") or "-")
    )

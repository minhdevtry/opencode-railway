#!/usr/bin/env python3
"""Tóm tắt các file log thô của đợt đo chiều model.

Đọc logs/limitshape_<cell>.jsonl, in: tổng request, số 200, chỉ số (index) request đầu tiên bị
429 HẠN MỪC (FreeUsageLimitError, hoặc 429 có Retry-After), và phân rã theo model.
"""
import glob
import json
import os
import sys

LOGDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def is_quota(d):
    err = d.get("err") or ""
    if "FreeUsageLimitError" in err:
        return True
    return d.get("status") == 429 and bool(d.get("ra"))


def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def main():
    cells = sys.argv[1:] or sorted(
        os.path.basename(p)[len("limitshape_"):-len(".jsonl")]
        for p in glob.glob(os.path.join(LOGDIR, "limitshape_*.jsonl"))
    )
    for cell in cells:
        path = os.path.join(LOGDIR, "limitshape_%s.jsonl" % cell)
        if not os.path.exists(path):
            print("== %-6s (KHONG CO FILE)" % cell)
            continue
        rows = load(path)
        n = len(rows)
        ok = sum(1 for d in rows if d.get("status") == 200)
        quota_idx = [d["i"] for d in rows if is_quota(d)]
        codes = {}
        for d in rows:
            codes[d.get("status")] = codes.get(d.get("status"), 0) + 1
        first = quota_idx[0] if quota_idx else None
        last = quota_idx[-1] if quota_idx else None
        print("== cell %s | requests=%d | 200=%d | codes=%s" % (cell, n, ok, dict(sorted(codes.items(), key=lambda kv: str(kv[0])))))
        if first is None:
            print("   khong co 429 han muc")
        else:
            d0 = next(d for d in rows if d.get("i") == first)
            print("   429 han muc dau tien tai i=%d (ra=%s, t=%s) | so request 429 han muc=%d (cuoi cung i=%d)"
                  % (first, d0.get("ra"), d0.get("t"), len(quota_idx), last))
        per = {}
        for d in rows:
            m = d.get("model", "?")
            s = per.setdefault(m, {"200": 0, "quota": 0, "n": 0, "first_quota": None})
            s["n"] += 1
            if d.get("status") == 200:
                s["200"] += 1
            if is_quota(d):
                s["quota"] += 1
                if s["first_quota"] is None:
                    s["first_quota"] = d["i"]
        for m, s in sorted(per.items()):
            print("   model=%-28s n=%-4d 200=%-4d 429hanmuc=%-4d first429i=%s"
                  % (m, s["n"], s["200"], s["quota"], s["first_quota"]))
        # Ô xen kẽ: model nào còn 200 SAU khi model kia đã 429 hạn mức lần đầu?
        if len(per) > 1:
            firsts = {m: s["first_quota"] for m, s in per.items() if s["first_quota"] is not None}
            if firsts:
                dead_model = min(firsts, key=lambda m: firsts[m])
                at = firsts[dead_model]
                others = [m for m in per if m != dead_model]
                for o in others:
                    after_ok = sum(1 for d in rows if d.get("model") == o and d.get("status") == 200 and d.get("i", 0) > at)
                    after_q = sum(1 for d in rows if d.get("model") == o and is_quota(d) and d.get("i", 0) > at)
                    print("   sau khi %s 429 han muc tai i=%d: %s -> %d request 200, %d 429 han muc"
                          % (dead_model, at, o, after_ok, after_q))


if __name__ == "__main__":
    main()

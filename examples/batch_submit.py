#!/usr/bin/env python3
"""Xử lý lô rất lớn qua /v1/batches (lưu bền trong Redis, gateway tự chạy hết tốc độ, lỗi tạm thời tự thử lại).

Client không phải giữ hàng nghìn kết nối hay tự retry: gửi lô 1 lần, quay lại lấy kết quả (theo đúng thứ tự gửi).
Ví dụ:  python3 batch_submit.py input.jsonl output.jsonl --model auto
input.jsonl: mỗi dòng {"id": "...", "prompt": "...", "system": "...(tuỳ chọn)"}
"""
import argparse, json, time, urllib.request

def call(url, body=None, timeout=120):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input"); ap.add_argument("output")
    ap.add_argument("--base", default="http://127.0.0.1:8090")
    ap.add_argument("--model", default="auto")
    ap.add_argument("--max-tokens", type=int, default=2048)
    ap.add_argument("--chunk", type=int, default=5000, help="số request mỗi lô gửi lên")
    a = ap.parse_args()

    items = [json.loads(l) for l in open(a.input, encoding="utf-8") if l.strip()]
    batches = []
    for i in range(0, len(items), a.chunk):
        reqs = [{"custom_id": str(it.get("id", i + j)), "body": {"model": a.model, "max_tokens": a.max_tokens, "messages":
                ([{"role": "system", "content": it["system"]}] if it.get("system") else []) + [{"role": "user", "content": it["prompt"]}]}}
                for j, it in enumerate(items[i:i + a.chunk])]
        b = call(a.base + "/v1/batches", {"requests": reqs})
        batches.append(b["id"]); print("đã gửi", b["id"], b["total"], "request", flush=True)

    t0 = time.time()
    while True:
        st = [call(f"{a.base}/v1/batches/{b}") for b in batches]
        done = sum(s["done"] + s["failed"] for s in st); total = sum(s["total"] for s in st)
        print(f"{time.time() - t0:6.0f}s  xong {done}/{total}  ({done / max(1, time.time() - t0):.1f} req/s)", flush=True)
        if all(s["status"] == "completed" for s in st):
            break
        time.sleep(10)

    with open(a.output, "w", encoding="utf-8") as out:
        for b in batches:
            off = 0
            while True:
                page = call(f"{a.base}/v1/batches/{b}/results?offset={off}&limit=1000")["data"]
                for x in page:
                    resp = x.get("response") or {}
                    msg = (resp.get("choices") or [{}])[0].get("message", {})
                    out.write(json.dumps({"id": x["custom_id"], "status": x["status"], "model": resp.get("model"),
                                          "content": msg.get("content"), "error": x.get("error")}, ensure_ascii=False) + "\n")
                if len(page) < 1000:
                    break
                off += 1000

if __name__ == "__main__":
    main()

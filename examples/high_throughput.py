#!/usr/bin/env python3
"""Gửi thật nhiều request đồng bộ với thông lượng cao (chỉ dùng thư viện chuẩn).

- Tự lấy mức đồng thời khuyến nghị từ GET /v1/capacity (đọc lại mỗi 30 s).
- Mỗi request 1 luồng; gateway tự xếp hàng phần vượt sức chứa nên KHÔNG cần retry 429 dày đặc.
- Chỉ retry khi lỗi mạng / 5xx, có backoff.

Ví dụ:  python3 high_throughput.py input.jsonl output.jsonl --model auto
input.jsonl: mỗi dòng {"id": "...", "prompt": "...", "system": "...(tuỳ chọn)"}
"""
import argparse, json, random, threading, time, urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

def capacity(base):
    try:
        with urllib.request.urlopen(base + "/v1/capacity", timeout=10) as r:
            return max(1, int(json.load(r)["recommended_concurrency"]))
    except Exception:
        return 50

def chat(base, body, timeout, retries=4):
    data = json.dumps(body).encode()
    for attempt in range(retries + 1):
        req = urllib.request.Request(base + "/v1/chat/completions", data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r), r.headers.get("X-Gateway-Model")
        except urllib.error.HTTPError as e:
            if e.code < 500 and e.code != 429:
                raise  # lỗi do request (400...): sửa request, không retry
            err = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        time.sleep(min(30, 2 ** attempt) + random.random())
    raise err

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input"); ap.add_argument("output")
    ap.add_argument("--base", default="http://127.0.0.1:8090")
    ap.add_argument("--model", default="auto", help="auto | auto-fast | auto-best | auto-long | deepseek | glm | ...")
    ap.add_argument("--max-tokens", type=int, default=2048)
    ap.add_argument("--effort", default=None, help="none | low | medium | high (bật/tắt suy nghĩ)")
    ap.add_argument("--timeout", type=int, default=1800, help="giây; gateway có thể giữ request trong hàng chờ tới 30 phút")
    a = ap.parse_args()

    items = [json.loads(l) for l in open(a.input, encoding="utf-8") if l.strip()]
    conc = capacity(a.base)
    sem = threading.BoundedSemaphore(conc * 4)  # trần cứng phía client (gateway vẫn tự xếp hàng phần dư)
    limit = {"n": conc}
    lock = threading.Lock(); inflight = {"n": 0}

    def refresh():
        while True:
            time.sleep(30)
            limit["n"] = capacity(a.base)

    threading.Thread(target=refresh, daemon=True).start()

    def work(it):
        while True:  # giữ số request đang chạy quanh mức khuyến nghị hiện tại
            with lock:
                if inflight["n"] < limit["n"]:
                    inflight["n"] += 1
                    break
            time.sleep(0.05)
        try:
            msgs = ([{"role": "system", "content": it["system"]}] if it.get("system") else []) + [{"role": "user", "content": it["prompt"]}]
            body = {"model": a.model, "messages": msgs, "max_tokens": a.max_tokens}
            if a.effort:
                body["reasoning_effort"] = a.effort
            with sem:
                r, used = chat(a.base, body, a.timeout)
            c = r["choices"][0]
            return {"id": it.get("id"), "model": used, "finish": c.get("finish_reason"), "content": c["message"].get("content")}
        except Exception as e:
            return {"id": it.get("id"), "error": str(e)[:300]}
        finally:
            with lock:
                inflight["n"] -= 1

    t0, done = time.time(), 0
    with ThreadPoolExecutor(max_workers=conc * 4) as pool, open(a.output, "w", encoding="utf-8") as out:
        for f in as_completed([pool.submit(work, it) for it in items]):
            out.write(json.dumps(f.result(), ensure_ascii=False) + "\n"); done += 1
            if done % 50 == 0 or done == len(items):
                print(f"{done}/{len(items)}  {done / (time.time() - t0):.1f} req/s  (đồng thời {limit['n']})", flush=True)

if __name__ == "__main__":
    main()

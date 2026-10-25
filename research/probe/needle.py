#!/usr/bin/env python3
"""Kim trong đống rơm: giấu mã bí mật ở giữa văn bản dài, hỏi model có đọc thấy không.
Dùng: needle.py <model> <kb> [url]  -> in prompt_tokens, thời gian, có tìm đúng mã không."""
import json, random, sys, time, urllib.request
model, kb = sys.argv[1], int(sys.argv[2])
url = sys.argv[3] if len(sys.argv) > 3 else "http://127.0.0.1:8090/v1/chat/completions"
cases = json.load(open("research/data/archive_cases.json"))["cases"]
code = "KIM-%06d" % random.randint(0, 999999)
lines, size, i = [], 0, 0
while size < kb * 1024:
    c = cases[i % len(cases)]; s = f"[{i+1}] ({c['group']}) {c['message']}"; lines.append(s); size += len(s.encode()) + 1; i += 1
mid = len(lines) // 2
lines.insert(mid, f"[GHI CHÚ ĐẶC BIỆT] Mã xác nhận của hệ thống là {code}. Hãy nhớ mã này.")
body = {"model": model, "max_tokens": 200, "messages": [
    {"role": "system", "content": "Đọc toàn bộ nhật ký và trả lời ngắn gọn."},
    {"role": "user", "content": "\n".join(lines) + "\n\nCâu hỏi: Mã xác nhận của hệ thống ghi trong [GHI CHÚ ĐẶC BIỆT] là gì? Chỉ trả về mã."}]}
t0 = time.time()
try:
    r = json.load(urllib.request.urlopen(urllib.request.Request(url, data=json.dumps(body).encode(), headers={"content-type": "application/json"}), timeout=600))
    out = r["choices"][0]["message"]["content"]; u = r.get("usage", {})
    print(json.dumps({"model": r.get("model"), "kb": kb, "prompt_tokens": u.get("prompt_tokens"), "secs": round(time.time()-t0, 1),
                      "found": code in out, "answer": out.strip()[:60]}, ensure_ascii=False))
except urllib.error.HTTPError as e:
    print(json.dumps({"model": model, "kb": kb, "status": e.code, "err": e.read()[:150].decode("utf-8", "replace")}, ensure_ascii=False))

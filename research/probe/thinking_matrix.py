#!/usr/bin/env python3
"""Ma trận tham số thinking: model x cách điều khiển -> HTTP, số ký tự suy nghĩ, nội dung, thời gian."""
import json, sys, time, urllib.request
URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8090/v1/chat/completions"
MODELS = ["deepseek-v4.1-flash-free", "glm-5.3-flash-free", "mimo-v2.6-flash-free", "nemotron-3.5-lightning-free", "big-pickle", "beam-free"]
VARIANTS = [("mặc định", {}), ("effort=low", {"reasoning_effort": "low"}), ("effort=high", {"reasoning_effort": "high"}),
            ("reasoning.effort=high", {"reasoning": {"effort": "high"}}), ("reasoning off", {"reasoning": {"enabled": False}}),
            ("thinking on", {"thinking": {"type": "enabled"}}), ("thinking off", {"thinking": {"type": "disabled"}})]
Q = "Một lớp có 3 ca học/ngày, mỗi ca 45 phút, nghỉ giữa ca 15 phút, bắt đầu 7h30. Ca 3 kết thúc lúc mấy giờ? Trả lời ngắn."
for m in MODELS:
    for name, extra in VARIANTS:
        body = {"model": m, "max_tokens": 4000, "messages": [{"role": "user", "content": Q}], **extra}
        t0 = time.time()
        try:
            r = json.load(urllib.request.urlopen(urllib.request.Request(URL, data=json.dumps(body).encode(), headers={"content-type": "application/json", "X-Model-Fallback": "deny"}), timeout=300))
            msg = r["choices"][0]["message"]
            print(f"{m[:22]:22} {name:22} 200 {time.time()-t0:5.1f}s think={len(msg.get('reasoning_content') or ''):5} content={(msg.get('content') or '').strip()[:30]!r} model={r.get('model','')[:12]}", flush=True)
        except urllib.error.HTTPError as e:
            print(f"{m[:22]:22} {name:22} {e.code} {e.read()[:90]}", flush=True)
        except Exception as e:
            print(f"{m[:22]:22} {name:22} ERR {e}", flush=True)

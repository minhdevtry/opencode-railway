#!/usr/bin/env python3
"""Gửi 1 request giả danh opencode CLI tới Zen qua 1 proxy. In status, retry-after, body ngắn, thời gian.
Dùng: probe.py <proxy|direct> <model> [prompt]   proxy dạng socks5h://127.0.0.1:1080"""
import json, os, random, secrets, subprocess, sys, time

ALPHA = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

def typeid(prefix, descending):
    ms = int(time.time() * 1000)
    val = (ms * 0x1000 + random.randint(1, 8)) & 0xFFFFFFFFFFFF
    if descending:
        val = (~val) & 0xFFFFFFFFFFFF
    return f"{prefix}_{val:012x}" + "".join(random.choice(ALPHA) for _ in range(14))

TOOLS = [{"type": "function", "function": {"name": n, "description": n, "parameters": {"type": "object", "properties": {k: {"type": "string"}}, "required": [k]}}}
         for n, k in (("bash", "command"), ("glob", "pattern"), ("grep", "pattern"), ("read", "filePath"))]

def probe(proxy, model, prompt="say pong in 1 word", max_tokens=16, timeout=60):
    ses = typeid("ses", True)
    body = {"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens,
            "stream": True, "tools": TOOLS}
    cmd = ["curl", "-sS", "-m", str(timeout), "-o", "-", "-D", "/dev/stderr", "-X", "POST",
           os.environ.get("URL","https://opencode.ai/zen/v1/chat/completions"),
           "-H", "Authorization: Bearer public", "-H", "Content-Type: application/json",
           "-H", f"User-Agent: {os.environ.get('UA','opencode/2.1.1')}", "-H", f"x-opencode-client: {os.environ.get('CLIENT','CLI')}",
           "-H", f"x-opencode-project: {os.environ.get('PROJECT') or secrets.token_hex(20)}", "-H", f"x-opencode-request: {typeid('msg', False)}",
           "-H", f"x-opencode-session: {ses}", "-H", f"x-opencode-session-id: {ses}", "-H", f"x-session-id: {ses}",
           "-H", "Accept: */*", "--data-binary", json.dumps(body, ensure_ascii=False)]
    if proxy != "direct":
        cmd[1:1] = ["-x", proxy]
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.time() - t0
    hdr = p.stderr
    status = 0
    ra = ""
    for line in hdr.splitlines():
        if line.startswith("HTTP/"):
            status = int(line.split()[1])
        elif line.lower().startswith("retry-after:"):
            ra = line.split(":", 1)[1].strip()
    out = p.stdout
    text = ""
    if status == 200:
        for line in out.splitlines():
            if line.startswith("data: ") and line[6:].strip() not in ("", "[DONE]"):
                try:
                    d = json.loads(line[6:])
                    text += (d["choices"][0]["delta"].get("content") or "") if d.get("choices") else ""
                except Exception:
                    pass
    return {"status": status, "retry_after": ra, "secs": round(dt, 2), "text": text[:80],
            "body": "" if status == 200 else out[:200], "curl_err": p.stderr[-200:] if status == 0 else ""}

if __name__ == "__main__":
    print(json.dumps(probe(sys.argv[1], sys.argv[2], *(sys.argv[3:4] or [])), ensure_ascii=False))

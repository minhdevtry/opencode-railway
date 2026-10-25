#!/usr/bin/env python3
"""Probe dạng opencode 2.1.4 (đúng header + 12 tool thật). Đọc IP thật mà opencode.ai thấy trên CÙNG kết nối
(curl --next tái dùng kết nối) để biết request đi ra WAN/egress nào.
Dùng: probe2.py <proxy|direct> <model> [n]"""
import json, os, random, secrets, subprocess, sys, time
ALPHA = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
HERE = os.path.dirname(os.path.abspath(__file__))
TOOLS = json.load(open(os.path.join(HERE, "tools_2.1.4.json")))
URL = os.environ.get("URL", "https://opencode.ai/inference/openai/v1/chat/completions")

def ses_id():
    ms = int(time.time() * 1000)
    val = (~((ms * 0x1000 + random.randint(1, 8)) & 0xFFFFFFFFFFFF)) & 0xFFFFFFFFFFFF
    return f"ses_{val:012x}" + "".join(random.choice(ALPHA) for _ in range(14))

def probe(proxy, model, prompt="say pong in one word", project=None):
    ses = ses_id()
    tid, sid = secrets.token_hex(16), secrets.token_hex(8)
    body = {"model": model, "messages": [{"role": "system", "content": "You are opencode, an interactive CLI coding agent."},
            {"role": "user", "content": prompt}], "tools": TOOLS, "stream": True,
            "stream_options": {"include_usage": True}, "max_completion_tokens": 256}
    hdrs = {"Authorization": "Bearer public", "Content-Type": "application/json",
            "User-Agent": os.environ.get("UA", "opencode/latest/2.1.4/cli"),
            "b3": f"{tid}-{sid}-1-{secrets.token_hex(8)}", "traceparent": f"00-{tid}-{sid}-01",
            "x-opencode-client": "cli", "x-opencode-project": project or os.environ.get("PROJECT") or secrets.token_hex(20),
            "x-opencode-session": ses, "x-opencode-session-id": ses, "x-session-affinity": ses, "x-session-id": ses,
            "Accept": "*/*"}
    hargs = sum((["-H", f"{k}: {v}"] for k, v in hdrs.items()), [])
    px = [] if proxy == "direct" else ["-x", proxy]
    cmd = ["curl", "-sS", "-m", "90", *px, "https://opencode.ai/cdn-cgi/trace", "--next", *px, "-m", "90",
           "-D", "/dev/stderr", "-X", "POST", URL, *hargs, "--data-binary", json.dumps(body, ensure_ascii=False)]
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.time() - t0
    ip = next((l[3:] for l in p.stdout.splitlines() if l.startswith("ip=")), "?")
    status, ra = 0, ""
    for line in p.stderr.splitlines():
        if line.startswith("HTTP/"): status = int(line.split()[1])
        elif line.lower().startswith("retry-after:"): ra = line.split(":", 1)[1].strip()
    text, err = "", ""
    for line in p.stdout.splitlines():
        if line.startswith("data: ") and line[6:].strip() not in ("", "[DONE]"):
            try:
                d = json.loads(line[6:]); text += (d["choices"][0]["delta"].get("content") or "") if d.get("choices") else ""
            except Exception: pass
        elif line.startswith("{"):
            err = line[:160]
    return {"ip": ip, "status": status, "ra": ra, "secs": round(dt, 2), "text": text[:60], "err": err}

if __name__ == "__main__":
    for _ in range(int(sys.argv[3]) if len(sys.argv) > 3 else 1):
        print(json.dumps(probe(sys.argv[1], sys.argv[2]), ensure_ascii=False), flush=True)

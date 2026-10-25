#!/usr/bin/env python3
"""Gửi body dựa trên request thật của opencode 1.18.35 (real_1.18_body.json) với các biến thể system prompt.
Dùng: replay.py <proxy|direct> <variant> [model]
variant: full | prefix:<N> | prefix:<N>+client | prefix:<N>+clientsys | client"""
import json, os, random, secrets, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ALPHA = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE = json.load(open(os.path.join(HERE, "real_1.18_body.json")))
REAL_SYS = BASE["messages"][0]["content"]
CLIENT_SYS = 'Phân loại tin nhắn. Trả về JSON duy nhất: {"category": "...", "is_urgent": false}'
def tid(prefix, desc):
    v = (int(time.time()*1000) * 0x1000 + random.randint(1, 8)) & 0xFFFFFFFFFFFF
    if desc: v = (~v) & 0xFFFFFFFFFFFF
    return f"{prefix}_{v:012x}" + "".join(random.choice(ALPHA) for _ in range(14))
def build(variant, model):
    b = json.loads(json.dumps(BASE)); b["model"] = model; b["max_tokens"] = 256
    user = "say pong in one word"
    if variant.startswith("pad:"):
        n = int(variant.split(":")[1]); sysc = ("Bạn là trợ lý phân loại tin nhắn vận hành trường học. " * 400)[:n]
    elif variant.startswith("mid:"):
        a, z = map(int, variant.split(":")[1].split("-")); sysc = REAL_SYS[a:z]
    elif variant.startswith("env"):
        i = REAL_SYS.index("You are powered by"); j = REAL_SYS.index("</env>") + len("</env>")
        envb = REAL_SYS[i:j].replace("glm-5.3-flash-free", model)
        k = int(variant.split(":")[1].split("+")[0]) if ":" in variant else 0
        sysc = (REAL_SYS[:k] + ("\n\n" if k else "") + envb)
        if variant.endswith("+client"): sysc += "\n\n" + CLIENT_SYS; user = 'Tin nhắn: "lớp 5A mất mic ngay bây giờ"'
    elif variant in ("powered", "envtag", "client+env", "env-client-user"):
        i = REAL_SYS.index("You are powered by"); j = REAL_SYS.index("</env>") + len("</env>")
        envb = REAL_SYS[i:j].replace("glm-5.3-flash-free", model)
        pw = envb[:envb.index("Here is some")].strip()
        tag = envb[envb.index("Here is some"):]
        sysc = {"powered": pw, "envtag": tag, "client+env": CLIENT_SYS + "\n\n" + envb, "env-client-user": envb}[variant]
        user = 'Tin nhắn: "lớp 5A mất mic ngay bây giờ"' if "client" in variant else user
        if variant == "env-client-user": user = CLIENT_SYS + "\n\n" + user
    elif variant == "full": sysc = REAL_SYS
    elif variant == "client": sysc = CLIENT_SYS
    else:
        n = int(variant.split(":")[1].split("+")[0]); sysc = REAL_SYS[:n]
    if variant.startswith(("prefix:", "pad:", "mid:")):
        if variant.endswith("+client"): sysc += "\n\n" + CLIENT_SYS; user = 'Tin nhắn: "lớp 5A mất mic ngay bây giờ"'
    msgs = [{"role": "system", "content": sysc}]
    if variant.endswith("+clientsys"): msgs.append({"role": "system", "content": CLIENT_SYS}); user = 'Tin nhắn: "lớp 5A mất mic ngay bây giờ"'
    msgs.append({"role": "user", "content": user}); b["messages"] = msgs
    return b
def send(proxy, variant, model="glm-5.3-flash-free"):
    ses = tid("ses", True)
    h = {"Authorization": "Bearer public", "Content-Type": "application/json",
         "User-Agent": "opencode/1.18.35 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14", "x-opencode-client": "cli",
         "x-opencode-project": "global", "x-opencode-request": tid("msg", False), "x-opencode-session": ses, "x-opencode-session-id": ses, "Accept": "*/*"}
    px = [] if proxy == "direct" else ["-x", proxy]
    cmd = ["curl", "-sS", "-m", "90", *px, "https://opencode.ai/cdn-cgi/trace", "--next", *px, "-m", "90", "-D", "/dev/stderr", "-X", "POST",
           "https://opencode.ai/inference/openai/v1/chat/completions", *sum((["-H", f"{k}: {v}"] for k, v in h.items()), []),
           "--data-binary", "@-"]
    if os.environ.get("DUMP"):
        json.dump({"url": "https://opencode.ai/inference/openai/v1/chat/completions", "headers": h, "body": json.dumps(build(variant, model), ensure_ascii=False)}, open(os.environ["DUMP"], "w"), ensure_ascii=False); return {}
    t0 = time.time(); p = subprocess.run(cmd, input=json.dumps(build(variant, model), ensure_ascii=False), capture_output=True, text=True)
    ip = next((l[3:] for l in p.stdout.splitlines() if l.startswith("ip=")), "?")
    st = next((int(l.split()[1]) for l in p.stderr.splitlines() if l.startswith("HTTP/")), 0)
    txt = ""; err = ""
    for l in p.stdout.splitlines():
        if l.startswith("data: ") and "[DONE]" not in l:
            try: d = json.loads(l[6:]); txt += (d["choices"][0]["delta"].get("content") or "") if d.get("choices") else ""
            except Exception: pass
        elif l.startswith("{"): err = l[:110]
    return {"v": variant, "ip": ip, "status": st, "secs": round(time.time()-t0, 2), "text": txt.replace("\n", " ")[:70], "err": err or p.stderr[-120:] if st != 200 else ""}
if __name__ == "__main__":
    print(json.dumps(send(sys.argv[1], sys.argv[2], *(sys.argv[3:4])), ensure_ascii=False))

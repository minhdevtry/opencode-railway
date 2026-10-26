#!/usr/bin/env python3
"""Sinh các file dump {url,headers,body} cho utlsprobe từ capture thật của client opencode.

- base_2.1.4.json : capture 2.1.4 (client mới nhất, 12 tools, có <env>)
- base_1.18.json  : capture 1.18.35 (project=global, 11 tools, có <env>)
- long.json       : base_2.1.4 + user message ~50KB
- short.json      : base_2.1.4 với user message ngắn
"""
import json
import os
import random
import string

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.join(HERE, "..", "probe")
OUT = os.path.join(HERE, "dumps")


def rnd(n):
    return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(n))


def load_cap(name, idx):
    path = os.path.join(PROBE, name)
    lines = open(path).read().splitlines()
    return json.loads(lines[idx])


def clean_headers(h):
    out = {}
    for k, v in h.items():
        lk = k.lower()
        if lk in ("b3", "traceparent", "content-length", "host", "connection", "accept-encoding"):
            continue
        out[k] = v
    # id request duy nhất theo từng request (client thật luôn gửi id mới)
    out["x-opencode-request"] = "msg_" + rnd(24)
    return out


def write(name, url, headers, body):
    p = os.path.join(OUT, name)
    with open(p, "w") as f:
        json.dump({"url": url, "headers": headers, "body": json.dumps(body, ensure_ascii=False)}, f)
    print("wrote", p, os.path.getsize(p), "bytes")


def main():
    os.makedirs(OUT, exist_ok=True)

    cap214 = load_cap("capture_2.1.4.jsonl", 1)
    h214 = clean_headers(cap214["headers"])
    b214 = json.loads(cap214["body"])
    write("base_2.1.4.json", cap214["url"], h214, b214)

    d = json.load(open(os.path.join(PROBE, "d.json")))
    h118 = clean_headers(d["headers"])
    b118 = json.loads(d["body"])
    write("base_1.18.json", d["url"], h118, b118)

    # bien the prompt: ngan / dai
    short = json.loads(json.dumps(b214))
    for m in short["messages"]:
        if m.get("role") == "user":
            m["content"] = "say pong in one word"
    write("short.json", cap214["url"], h214, short)

    long_ = json.loads(json.dumps(b214))
    filler = ("The quick brown fox jumps over the lazy dog. " * 900)  # ~40KB
    for m in long_["messages"]:
        if m.get("role") == "user":
            m["content"] = "Read the following text and say pong in one word.\n\n" + filler
    write("long.json", cap214["url"], h214, long_)

    # bien the dung tool -> rơi vào bucket shared (thiếu 1 trong 3 điều kiện client thật)
    notools = json.loads(json.dumps(b214))
    notools.pop("tools", None)
    write("notools.json", cap214["url"], h214, notools)

    # bien thieu khung <env>
    noenv = json.loads(json.dumps(b214))
    for m in noenv["messages"]:
        if m.get("role") in ("system", "developer") and isinstance(m.get("content"), str):
            i = m["content"].find("<env>")
            if i >= 0:
                j = m["content"].find("</env>", i)
                m["content"] = m["content"][:i] + m["content"][j + len("</env>"):]
    write("noenv.json", cap214["url"], h214, noenv)


if __name__ == "__main__":
    main()

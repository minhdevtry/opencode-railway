#!/usr/bin/env python3
"""Sinh dump request giống hệt request mà gateway (profile 1.18) gửi lên opencode.ai.

Gateway (main.go applyFingerprint + upstream.go setUpstreamHeaders, mặc định):
  headers: Authorization: Bearer public, Content-Type: application/json,
           User-Agent: opencode/1.18.35 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14,
           x-opencode-client: cli, x-opencode-project: global,
           x-opencode-request: <id mới>, x-opencode-session/-id: <ses>, Accept: */*
  body:    {model, messages, stream:true, tools:[bash,glob,grep,read]}
           + system message chỉ chứa khối <env> (main.go envBlock)
"""
import json
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "dumps")
URL = "https://opencode.ai/inference/openai/v1/chat/completions"
MODEL = "glm-5.3-flash-free"

ENV_BLOCK = (
    "You are powered by the model named " + MODEL + ". The exact model ID is opencode/" + MODEL + "\n"
    "Here is some useful information about the environment you are running in:\n<env>\n"
    "  Working directory: /home/user/project\n  Workspace root folder: /home/user/project\n"
    "  Is directory a git repo: no\n  Platform: linux\n  Today's date: "
    + time.strftime("%a %b %d %Y", time.gmtime())
    + "\n</env>"
)


def tool(name, props, req):
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": name,
            "strict": False,
            "parameters": {
                "type": "object",
                "properties": {p: {"type": "string"} for p in props},
                "required": req,
            },
        },
    }


MANDATORY = [
    tool("bash", ["command"], ["command"]),
    tool("glob", ["pattern"], ["pattern"]),
    tool("grep", ["pattern"], ["pattern"]),
    tool("read", ["filePath"], ["filePath"]),
]


def headers(ses, reqid):
    return {
        "Authorization": "Bearer public",
        "Content-Type": "application/json",
        "User-Agent": "opencode/1.18.35 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-request": reqid,
        "x-opencode-session": ses,
        "x-opencode-session-id": ses,
        "Accept": "*/*",
    }


def write(name, body, hdr):
    p = os.path.join(OUT, name)
    with open(p, "w") as f:
        json.dump({"url": URL, "headers": hdr, "body": json.dumps(body, ensure_ascii=False)}, f)
    print("wrote", p, os.path.getsize(p))


def main():
    # gw1: y hệt gateway (system chỉ có env, 4 tools stub)
    b = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": ENV_BLOCK},
            {"role": "user", "content": "say pong in one word"},
        ],
        "stream": True,
        "tools": MANDATORY,
    }
    write("gw1.json", b, headers("ses_gwshape_0000000000000000000000", "msg_000000000000000000000001"))

    # gw2: như gw1 nhưng giữ cả 11 tools thật của client 1.18
    cap = json.loads(open(os.path.join(HERE, "..", "probe", "d.json")).read())
    real_tools = json.loads(cap["body"])["tools"]
    b2 = json.loads(json.dumps(b))
    b2["tools"] = real_tools
    write("gw2.json", b2, headers("ses_gwshape_tools_0000000000000000", "msg_000000000000000000000002"))

    # gw3: như gw1 nhưng system prompt đầy đủ của client 2.1.4 (có env) + 12 tools thật
    cap214 = json.loads(open(os.path.join(HERE, "..", "probe", "capture_2.1.4.jsonl")).read().splitlines()[1])
    body214 = json.loads(cap214["body"])
    b3 = json.loads(json.dumps(body214))
    write("gw3.json", b3, headers("ses_gwshape_full_00000000000000000", "msg_000000000000000000000003"))


if __name__ == "__main__":
    main()

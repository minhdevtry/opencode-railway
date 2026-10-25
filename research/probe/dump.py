import json
from mitmproxy import http
def request(flow: http.HTTPFlow):
    if "opencode.ai" in flow.request.pretty_host and flow.request.method == "POST":
        with open("capture.jsonl", "a") as f:
            f.write(json.dumps({"url": flow.request.pretty_url, "headers": dict(flow.request.headers), "body": flow.request.get_text()}) + "\n")
def response(flow: http.HTTPFlow):
    if "opencode.ai" in flow.request.pretty_host and flow.request.method == "POST":
        with open("capture_resp.jsonl", "a") as f:
            f.write(json.dumps({"url": flow.request.pretty_url, "status": flow.response.status_code, "headers": dict(flow.response.headers)}) + "\n")

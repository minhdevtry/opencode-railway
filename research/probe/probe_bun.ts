// Probe bằng Bun fetch (TLS stack BoringSSL giống opencode CLI). Dùng: bun probe_bun.ts <model> [n] [proxyHttpUrl] [ua]
const tools = await Bun.file(import.meta.dir + "/tools_2.1.4.json").json();
const ALPHA = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const rnd = (n: number) => Array.from({ length: n }, () => ALPHA[Math.floor(Math.random() * 62)]).join("");
const hex = (n: number) => Array.from(crypto.getRandomValues(new Uint8Array(n)), b => b.toString(16).padStart(2, "0")).join("");
function sesId() {
  const v = (~((BigInt(Date.now()) * 0x1000n + BigInt(1 + Math.floor(Math.random() * 8))) & 0xFFFFFFFFFFFFn)) & 0xFFFFFFFFFFFFn;
  return "ses_" + v.toString(16).padStart(12, "0") + rnd(14);
}
const [model = "glm-5.3-flash-free", n = "1", proxy, ua = "opencode/1.18.35 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"] = Bun.argv.slice(2);
const url = process.env.URL ?? "https://opencode.ai/inference/openai/v1/chat/completions";
for (let i = 0; i < Number(n); i++) {
  const ses = sesId();
  const legacy = ua.startsWith("opencode/1.");
  const headers: Record<string, string> = {
    Authorization: "Bearer public", "Content-Type": "application/json", "User-Agent": ua,
    "x-opencode-client": "cli", "x-opencode-project": process.env.PROJECT ?? "global",
    "x-opencode-session": ses, "x-opencode-session-id": ses,
  };
  if (legacy) headers["x-opencode-request"] = "msg_" + rnd(26);
  else Object.assign(headers, { "x-session-affinity": ses, "x-session-id": ses });
  const body = { model, messages: [{ role: "system", content: "You are opencode, an interactive CLI coding agent." }, { role: "user", content: "say pong in one word" }],
    tools, stream: true, stream_options: { include_usage: true }, max_tokens: 256 };
  const t0 = performance.now();
  const opts: any = { method: "POST", headers, body: JSON.stringify(body) };
  if (proxy) opts.proxy = proxy;
  try {
    const r = await fetch(url, opts);
    const txt = await r.text();
    let out = "";
    for (const line of txt.split("\n")) if (line.startsWith("data: ") && !line.includes("[DONE]")) { try { out += JSON.parse(line.slice(6)).choices?.[0]?.delta?.content ?? ""; } catch {} }
    console.log(JSON.stringify({ status: r.status, ra: r.headers.get("retry-after"), secs: ((performance.now() - t0) / 1000).toFixed(2), text: out.slice(0, 50), err: r.status === 200 ? "" : txt.slice(0, 120) }));
  } catch (e) { console.log(JSON.stringify({ status: 0, err: String(e).slice(0, 150) })); }
}

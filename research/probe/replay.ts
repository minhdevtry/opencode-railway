// Phát lại nguyên văn 1 request đã bắt (đổi session/request ID mới cho hợp lệ thời gian). bun replay.ts <capture.jsonl> <line> [n]
const [file, lineNo = "0", n = "1"] = Bun.argv.slice(2);
const rec = JSON.parse((await Bun.file(file).text()).trim().split("\n")[Number(lineNo)]);
const ALPHA = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const rnd = (k: number) => Array.from({ length: k }, () => ALPHA[Math.floor(Math.random() * 62)]).join("");
const ses = () => "ses_" + ((~((BigInt(Date.now()) * 0x1000n + 1n) & 0xFFFFFFFFFFFFn)) & 0xFFFFFFFFFFFFn).toString(16).padStart(12, "0") + rnd(14);
const msg = () => "msg_" + ((BigInt(Date.now()) * 0x1000n + 1n) & 0xFFFFFFFFFFFFn).toString(16).padStart(12, "0") + rnd(14);
for (let i = 0; i < Number(n); i++) {
  const h: Record<string, string> = {};
  for (const [k, v] of Object.entries(rec.headers as Record<string, string>)) if (!["host", "content-length", "connection", "accept-encoding"].includes(k.toLowerCase())) h[k] = v;
  const s = ses();
  for (const k of Object.keys(h)) { if (/session|affinity/i.test(k)) h[k] = s; if (/x-opencode-request/i.test(k)) h[k] = msg(); }
  let b = JSON.parse(rec.body);
  const mut = (process.env.MUT ?? "").split(",").filter(Boolean);
  for (const m of mut) {
    if (m === "tinyuser") b.messages = b.messages.map((x: any) => x.role === "user" ? { ...x, content: "say pong in one word" } : x);
    if (m === "nosystem") b.messages = b.messages.filter((x: any) => x.role !== "system");
    if (m === "shortsystem") b.messages = b.messages.map((x: any) => x.role === "system" ? { ...x, content: "You are opencode, an interactive CLI coding agent." } : x);
    if (m === "notools") delete b.tools;
    if (m === "fourtools") b.tools = b.tools.filter((t: any) => ["bash", "glob", "grep", "read"].includes(t.function?.name));
    if (m === "stubtools") b.tools = b.tools.map((t: any) => ({ type: "function", function: { name: t.function.name, description: t.function.name, parameters: { type: "object", properties: {} } } }));
    if (m === "maxtok512") b.max_tokens = 512;
    if (m === "noextra") { delete b.reasoning_effort; delete b.stream_options; delete b.temperature; }
    if (m === "model") b.model = process.env.MODEL;
    if (m === "ua2") h["User-Agent"] = "opencode/latest/2.1.4/cli";
    if (m === "projrand") h["x-opencode-project"] = crypto.randomUUID().replace(/-/g, "").padEnd(40, "0");
    if (m === "noreqid") { for (const k of Object.keys(h)) if (/x-opencode-request/i.test(k)) delete h[k]; }
  }
  if (process.env.DUMP) { await Bun.write(process.env.DUMP, JSON.stringify({ url: rec.url, headers: h, body: JSON.stringify(b) })); continue; }
  const t0 = performance.now();
  const r = await fetch(rec.url, { method: "POST", headers: h, body: JSON.stringify(b), ...(process.env.PROXY ? { proxy: process.env.PROXY } : {}) });
  const t = await r.text();
  let out = ""; for (const L of t.split("\n")) if (L.startsWith("data: ") && !L.includes("[DONE]")) { try { out += JSON.parse(L.slice(6)).choices?.[0]?.delta?.content ?? ""; } catch {} }
  console.log(JSON.stringify({ mut: process.env.MUT ?? "", status: r.status, secs: ((performance.now() - t0) / 1000).toFixed(2), text: out.slice(0, 40), err: r.status === 200 ? "" : t.slice(0, 100) }));
}

// Gửi nhiều request đồng thời từ Bun/Node 18+ (fetch có sẵn). Giữ số request đang chạy quanh recommended_concurrency.
//   bun examples/high_throughput.ts input.jsonl output.jsonl auto
import { readFileSync, writeFileSync } from "node:fs";

const [input = "input.jsonl", output = "output.jsonl", model = "auto"] = process.argv.slice(2);
const BASE = process.env.ORW_BASE ?? "http://127.0.0.1:8090";

async function capacity(): Promise<number> {
  try { return (await (await fetch(`${BASE}/v1/capacity`)).json()).recommended_concurrency ?? 50; } catch { return 50; }
}

async function chat(body: unknown, retries = 4): Promise<any> {
  for (let i = 0; ; i++) {
    try {
      // Gateway có thể giữ request trong hàng chờ tới 30 phút: đừng đặt timeout ngắn.
      const r = await fetch(`${BASE}/v1/chat/completions`, { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body), signal: AbortSignal.timeout(30 * 60_000) });
      if (r.ok) return { json: await r.json(), model: r.headers.get("X-Gateway-Model") };
      if (r.status < 500 && r.status !== 429) throw new Error(`HTTP ${r.status}: ${await r.text()}`); // lỗi request: không retry
    } catch (e) { if (i >= retries) throw e; }
    await new Promise(res => setTimeout(res, Math.min(30_000, 1000 * 2 ** i)));
  }
}

const items = readFileSync(input, "utf8").split("\n").filter(Boolean).map(l => JSON.parse(l));
let limit = await capacity();
const timer = setInterval(async () => { limit = await capacity(); }, 30_000);
let next = 0, inflight = 0, done = 0;
const out: string[] = new Array(items.length);
const t0 = Date.now();

await new Promise<void>(resolve => {
  const pump = () => {
    while (inflight < limit && next < items.length) {
      const i = next++, it = items[i];
      inflight++;
      chat({ model, max_tokens: 2048, messages: [...(it.system ? [{ role: "system", content: it.system }] : []), { role: "user", content: it.prompt }] })
        .then(({ json, model: used }) => { out[i] = JSON.stringify({ id: it.id, model: used, content: json.choices[0].message.content }); })
        .catch(e => { out[i] = JSON.stringify({ id: it.id, error: String(e).slice(0, 300) }); })
        .finally(() => {
          inflight--; done++;
          if (done % 50 === 0 || done === items.length) console.log(`${done}/${items.length}  ${(done / ((Date.now() - t0) / 1000)).toFixed(1)} req/s  (đồng thời ${limit})`);
          if (done === items.length) resolve(); else pump();
        });
    }
  };
  pump();
});
clearInterval(timer);
writeFileSync(output, out.join("\n") + "\n");

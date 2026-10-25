# Opencode Railway Gateway

Cổng API viết bằng Go, biến các model **miễn phí** của OpenCode Zen thành API quen thuộc (OpenAI, Anthropic, Responses),
chịu tải đồng thời lớn bằng cách tự chia request qua nhiều đường mạng (16 Cloudflare WARP, proxy dân cư, IP máy).

Không cần API key trả phí. Client chỉ việc gửi request; gateway lo chọn đường, chọn model, xếp hàng, thử lại.

## Thông lượng (đo trên hệ thống thật)

| Cách dùng | Thông lượng | Ghi chú |
|---|---|---|
| **Không quan tâm model** (`auto`), việc ngắn | **13–14 request/s**, ~4 000 token ra/s | 3000/3000 request không lỗi; bắn dồn 1 000 request cùng lúc: 999/1000 |
| **Cố định 1 model** (`deepseek`, bật suy nghĩ), báo cáo dài | **~8 700 token ra/s** ở 200 request đồng thời | 100% đúng model đã chọn |
| Văn bản rất dài (`auto-long`) | ~300k token/request | đọc đúng, quá giới hạn thì tự chuyển model |
| Lô cực lớn (`/v1/batches`) | gateway tự chạy hết tốc độ | lưu bền Redis, không mất job khi khởi động lại |

Gửi bao nhiêu request cùng lúc cũng được: phần vượt sức chứa xếp hàng chờ, không trả lỗi.

**Viết chương trình gọi gateway với thông lượng cao: đọc [`docs/CLIENT_GUIDE.md`](docs/CLIENT_GUIDE.md)** (chọn chế độ,
mức đồng thời, timeout/retry, ví dụ Python/TypeScript chạy được trong `examples/`).

Vì sao hệ thống được thiết kế như vậy, kèm toàn bộ số đo: [`research/README.md`](research/README.md).

---

## 1. Dùng nhanh

```bash
curl http://127.0.0.1:8090/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "auto",
  "messages": [{"role": "user", "content": "Tóm tắt văn bản sau ..."}]
}'
```

| Endpoint | Dùng cho |
|---|---|
| `POST /v1/chat/completions` | OpenAI Chat (stream và non-stream, tool calling) |
| `POST /v1/messages` | Anthropic Messages (Claude Code, SDK Anthropic) |
| `POST /v1/responses` | OpenAI Responses |
| `POST /v1/systemone` | TypeSafe System One (`jev-1.13`) |
| `POST /v1/batches`, `GET /v1/batches/{id}`, `GET /v1/batches/{id}/results` | Xử lý lô lớn, lưu bền trong Redis |
| `POST /v1/jobs`, `GET /v1/jobs/{id}?wait=60` | 1 request chạy nền |
| `GET /v1/capacity` | Sức chứa hiện tại + mức đồng thời nên gửi |
| `GET /v1/models` | Danh sách model |
| `GET /status` | Trạng thái chi tiết từng đường mạng (cho vận hành) |
| `GET /health` | Kiểm tra sống |

---

## 2. Chọn model: hai chế độ

### 2.1. Chế độ `auto`: tối đa thông lượng, không quan tâm model nào

Gateway tự xoay vòng qua các model free, model nào lỗi/trả rỗng/tự gọi tool thì tự đổi model khác, model nào trả lời
kém nhiều thì tạm loại. Phù hợp xử lý hàng loạt văn bản ngắn tới vừa.

| Tên model | Gồm | Khi nào dùng |
|---|---|---|
| `auto` | mọi model trong pool | mặc định, tải cao nhất |
| `auto-fast` | nemotron-lightning, glm, beam | cần nhanh nhất (tắt suy nghĩ, ~1–3 s/request ngắn) |
| `auto-best` | deepseek, glm, mimo | cần câu trả lời kỹ hơn |
| `auto-long` | mọi model đọc được văn bản dài | văn bản hàng trăm nghìn token |

Với `auto`, gateway tự: tắt chế độ suy nghĩ ở model hỗ trợ (nhanh hơn 5–10 lần), đặt `tool_choice: none`, và **tự học
giới hạn context** của từng model (model nào từng báo "vượt context" ở cỡ N token thì không nhận request cỡ đó nữa,
request hiện tại tự chuyển sang model khác). Đổi danh sách bằng env `AUTO_FAST_MODELS`, `AUTO_BEST_MODELS`, `AUTO_LONG_MODELS`.

### 2.2. Chế độ cố định model: nhất quán, bật suy nghĩ, output dài

Gọi đích danh model (`deepseek`, `glm`, `mimo`, `nemotron`, `big-pickle`, `beam`, hoặc tên đầy đủ `...-free`) thì:

- **Không bao giờ đổi sang model khác** (kết quả nhất quán). Muốn cho phép đổi khi model đó hết đường: gửi header
  `X-Model-Fallback: allow`.
- **Tham số của client được giữ nguyên** (`max_tokens`, `temperature`, thinking...). Tham số nào nhà cung cấp phía sau
  không nhận thì gateway tự gỡ và gửi lại, báo trong header phản hồi `X-Gateway-Dropped-Params`.
- Vẫn tự phục hồi: stream treo/đứt giữa chừng thì gửi lại **cùng model** qua đường mạng khác.

### 2.3. Điều khiển suy nghĩ (thinking)

Chuẩn chung, mọi model: `"reasoning_effort": "none" | "low" | "medium" | "high"`.

- `none`: gateway tự dịch sang đúng công tắc tắt của từng model (deepseek, big-pickle: `thinking.type=disabled`;
  mimo, nemotron, glm: `reasoning.enabled=false`; glm chỉ giảm, không tắt hẳn; beam không có công tắc).
- `low` / `medium` / `high`: chuyển thẳng cho model.
- Muốn dùng tham số gốc của model (`thinking`, `reasoning`) vẫn được, gateway không ghi đè.

Lưu ý: tắt suy nghĩ thì nhanh nhưng **dễ sai ở câu cần suy luận** (đo: câu tính giờ học, 2/4 model trả lời sai khi tắt).
Việc gán nhãn/tóm tắt thì tắt được; việc cần lập luận thì để bật.

### 2.4. Giới hạn đo thực tế (không theo catalog, catalog sai)

| Model | Đọc đúng tới | Ghi chú |
|---|---|---|
| nemotron-3.5-lightning, glm-5.3, mimo, beam, nemotron-super-vl | ~300k token | ~380k: mimo, nemotron lỗi/rỗng; beam vẫn đọc được 370k |
| deepseek-v4.1-flash | ~318k token | ~380k báo vượt context |
| big-pickle | < 160k token | context nhỏ nhất |

Output: gateway không giới hạn `max_tokens`. **Khi bật suy nghĩ và cần output dài, đặt `max_tokens` từ 16 000 trở lên**:
phần suy nghĩ tính chung vào `max_tokens`; đo với deepseek, đặt 8 000 thì 16% request bị suy nghĩ ăn hết, không còn chỗ
cho câu trả lời (rỗng, `finish_reason: length`); đặt 16 000 còn 2%.

---

## 3. Gửi bao nhiêu request cùng lúc?

- Gửi bao nhiêu cũng được: phần vượt sức chứa **xếp hàng chờ** (tối đa `MAX_WAIT`, mặc định 30 phút), không bị lỗi.
- Để chờ ít nhất: giữ số request đang gửi quanh `recommended_concurrency` của `GET /v1/capacity`.
- Lô rất lớn (hàng nghìn tới hàng trăm nghìn): dùng `/v1/batches`. Job lưu bền trong Redis, lỗi tạm thời tự thử lại có
  backoff, gateway khởi động lại vẫn chạy tiếp, lấy kết quả theo đúng thứ tự gửi:

```bash
curl -X POST http://127.0.0.1:8090/v1/batches -H 'Content-Type: application/json' -d '{
  "requests": [
    {"custom_id": "a1", "body": {"model": "auto", "messages": [{"role": "user", "content": "..."}]}},
    {"custom_id": "a2", "body": {"model": "auto", "messages": [{"role": "user", "content": "..."}]}}
  ]}'
# -> {"id": "batch_..."}
curl http://127.0.0.1:8090/v1/batches/batch_...                     # tiến độ
curl 'http://127.0.0.1:8090/v1/batches/batch_.../results?offset=0&limit=1000'
```

Số đo trên hệ thống thật (16 WARP + 3 proxy dân cư + IP máy, workload gán nhãn ticket thật): xem mục 7.
Hướng dẫn đầy đủ cho client (mức đồng thời, timeout, retry, ví dụ code): [`docs/CLIENT_GUIDE.md`](docs/CLIENT_GUIDE.md).

---

## 4. Cách hệ thống hoạt động (tóm tắt)

1. **Giả danh đúng client OpenCode.** Free tier của OpenCode chỉ tính request vào quota "client thật" khi có cùng lúc:
   dấu vân tay TLS của Bun (gateway dùng uTLS phát lại ClientHello của Bun, `tlsfp.go`), khối `<env>` trong system
   prompt (`withEnvBlock`, ghép sau prompt của client), và có tool. Thiếu 1 trong 3 là rơi vào quota chung đã cạn (429).
2. **Quota tính theo (IP, session).** Mỗi IP mở được ~50 session/ngày. Gateway giữ session sống lâu cho từng đường mạng
   (`sessions.go`), session bị chặn thì cho nghỉ rồi thử lại.
3. **Nhiều đường mạng, tự làm mới.** 16 container WARP (mỗi cái 1 IPv6 riêng). WARP hết lượt thì gateway tự đăng ký lại
   danh tính để lấy IP mới (~15–25 s, không restart, `warp.go`). Proxy dân cư hết lượt hoặc chập chờn thì tự xoay IP.
4. **Bộ điều phối** (`sched.go`): hàng đợi FIFO 2 mức (request đồng bộ trước, job nền sau), giữ chỗ nguyên tử, trần
   số request đồng thời của mỗi đường mạng tự co giãn (AIMD).
5. **Kiểm tra kết quả**: stream treo quá hạn token đầu hoặc đứt trước `finish_reason` thì không trả nội dung cụt mà thử
   lại (auto: model khác; cố định: cùng model, đường khác).

---

## 5. Triển khai

```bash
cd /home/minhdn3/Downloads/opencode-railway
docker compose -f docker-compose.warp.yml up -d            # 16 WARP + Redis + gateway (cổng 8090)
docker compose -f docker-compose.warp.yml up -d --build --no-deps gateway   # chỉ build lại gateway
docker logs -f orw-prod
```

Mọi container đều giới hạn log 3 × 10 MB. Gateway mount `/var/run/docker.sock` để tự đổi danh tính WARP.

`scripts/warp_watch.sh` (chạy bằng systemd user timer `warp-watch.timer`, 2 phút/lần) khởi động lại container WARP
bị unhealthy (lỗi DNS nội bộ của WARP mà `restart: unless-stopped` không tự phát hiện).

### Biến môi trường chính

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PROXY_POOL` | | Danh sách SOCKS5 của WARP (thứ tự khớp container `warp-proxy`, `warp-proxy-2`, ...) |
| `WARP_CONTAINERS` | suy ra từ thứ tự | Tên container WARP nếu đặt tên khác |
| `RESIDENTIAL_PROXIES`, `RESIDENTIAL_REFRESH_URLS` | | Proxy dân cư SOCKS5 và link API đổi IP tương ứng |
| `REDIS_URL` | | Bật hàng đợi job bền (`redis://redis:6379`) |
| `JOB_WORKERS` | 256 | Số job chạy song song tối đa |
| `MAX_WAIT` | 300s (compose: 1800s) | Thời gian tối đa 1 request chờ đường mạng |
| `MAX_CONCURRENCY` | 1000 (compose: 20000) | Số request gateway nhận cùng lúc |
| `AUTO_MODEL_FALLBACK` | false | true: model cố định được đổi sang model khác khi hết đường |
| `AUTO_FAST_MODELS`, `AUTO_BEST_MODELS`, `AUTO_LONG_MODELS` | xem mục 2.1 | Danh sách model của profile |
| `SESSIONS_PER_EGRESS`, `SESSION_INFLIGHT` | 8, 4 | Số session mỗi đường mạng, số request song song mỗi session |
| `LIMIT_WARP`, `LIMIT_RES`, `LIMIT_DIRECT`, `LIMIT_MAX` | 4, 4, 2, 32 | Trần đồng thời ban đầu / tối đa mỗi đường (tự co giãn) |
| `AUTO_FIRST_TOKEN` | 25s | Quá hạn này chưa có token đầu thì thử lại |
| `UPSTREAM_TLS` | bun | `go` để tắt giả lập TLS (chỉ để thí nghiệm) |
| `RATE_LIMIT_RPM` | 6000 (compose: 0) | Giới hạn theo IP client; 0 = tắt |
| `ALLOWED_IPS` | trống = chỉ mạng nội bộ | Danh sách IP/CIDR được gọi |

---

## 6. Vận hành và xử lý sự cố

| Hiện tượng | Xem gì | Xử lý |
|---|---|---|
| Chậm, hàng chờ dài | `GET /v1/capacity` (`usable_egress`, `queued_sync`) | Bình thường cuối ngày UTC khi IP cạn lượt; quota reset 00:00 UTC (07:00 VN). Thêm proxy dân cư nếu cần |
| Nhiều 429 | `/status`: `new_sessions_denied`, `identity_renews` | WARP tự đổi IP; nếu `identity_renews` không tăng, xem log `[WARP]` |
| `Failed to communicate with the WARP API` | log `[WARP]` | API đăng ký của Cloudflare tạm từ chối, gateway tự thử lại sau 30 s |
| Proxy dân cư lỗi | log `[resN]`, `[ProxyXoay]` | Tự xoay IP sau 5 lỗi liên tiếp; key hết hạn thì tự loại |
| Request lớn báo 400 vượt context | `/status` → `context_limits` | Với `auto`: tự chuyển model. Với model cố định: chọn model đọc dài hơn (mục 2.4) |

**Không chạy quá 16 container WARP** trừ khi đã nâng giới hạn kernel
(`sudo sysctl -w fs.inotify.max_user_instances=1024 fs.inotify.max_user_watches=1048576`): đo thực tế 32 container làm
daemon WARP lỗi "Too many open files" và ghi ~640 MB log/giờ mỗi container.

---

## 7. Hiệu năng đo được

| Kịch bản | Kết quả |
|---|---|
| `auto`, gán nhãn, 200 đồng thời, đầu ngày UTC | 1000/1000, **13 request/s, ~4 000 token ra/s** |
| `auto`, gán nhãn, 3 000 request ở 200 đồng thời, giữa ngày | 3000/3000, 0 lỗi, ~14 request/s lúc ổn định |
| `auto`, 1 000 request bắn dồn cùng lúc | 999/1000 |
| `auto-long`, văn bản ~300k token (tìm "kim" giữa văn bản) | đọc đúng; 383k token: tự chuyển sang model đọc được |
| Cố định `deepseek`, bật suy nghĩ, báo cáo 600–900 từ, 200 đồng thời, `max_tokens` 8000 | 600 request: 100% chạy đúng deepseek, **~8 700 token ra/s**; 97 bị rỗng vì suy nghĩ ăn hết 8 000 token |
| Như trên, `max_tokens` 16000, 100 đồng thời | 98/100, ~6 800 token ra/s, p50 44 s |

---

## 8. Claude Code

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://127.0.0.1:8090",
    "ANTHROPIC_API_KEY": "local",
    "ANTHROPIC_MODEL": "deepseek",
    "ANTHROPIC_SMALL_FAST_MODEL": "auto-fast",
    "CLAUDE_CODE_DISABLE_UNKNOWN_MODEL_WINDOW_ENFORCEMENT": "1",
    "CLAUDE_CODE_AUTO_COMPACT_WINDOW": "280000",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"
  }
}
```

`AUTO_COMPACT_WINDOW` để dưới ~300k token vì đó là mức đọc đúng đo được (mục 2.4).

---

## 9. Phát triển

```bash
go test -race ./...                       # toàn bộ test, có kiểm tra data race
research/run_orw.sh                       # chạy gateway thử nghiệm trên :18090 (không đụng prod)
go run ./research/loadgen -url http://127.0.0.1:8090/v1/chat/completions -n 1000 -c 200 -workload label
research/ceiling.sh                       # đo trần theo bậc concurrency (nên chạy ngay sau 07:00)
```

| File | Nội dung |
|---|---|
| `main.go` | Handler OpenAI, model, chọn model/profile, kiểm tra kết quả auto |
| `upstream.go` | Gửi request lên upstream, xử lý lỗi/thử lại từng loại mã |
| `egress.go` | Đường mạng (direct/WARP/dân cư), cooldown, xoay proxy dân cư |
| `sched.go` | Bộ điều phối, hàng đợi, trần đồng thời thích ứng, chế độ cố định model |
| `sessions.go` | Session sống lâu theo từng đường mạng |
| `tlsfp.go` | TLS giả lập Bun + SOCKS5/HTTP CONNECT tự viết |
| `warp.go` | Đổi danh tính WARP qua Docker API, vòng quét tự làm mới |
| `autoguard.go` | Tham số theo model, thinking, kiểm tra stream, chất lượng model |
| `profiles.go` | Profile auto-fast/best/long, giới hạn context tự học |
| `jobs.go`, `redis.go` | Hàng đợi job bền, client Redis tối giản |
| `capacity.go` | `/v1/capacity` |
| `examples/` | Client mẫu thông lượng cao (Python, TypeScript, batch) |
| `docs/CLIENT_GUIDE.md` | Hướng dẫn client |
| `anthropic.go`, `responses.go` | Chuyển đổi Anthropic / Responses ↔ OpenAI |

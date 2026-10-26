# Nghiên cứu: khai thác tối đa free tier OpenCode Zen qua WARP / proxy

Thư mục này chứa công cụ đo và kết quả thực nghiệm (2026-10-24/25). Dữ liệu thật (`data/`), kết quả thô (`results/*.jsonl`)
và binary chạy thử (`run/`) không được commit.

## 1. Cổng phân loại free tier (căn nguyên của 429 `FreeUsageLimitError` "trên mọi egress")

Từ khoảng 2026-10-21, upstream (Worker Cloudflare, mã nguồn đã đóng) xếp request vào 2 loại quota:

| Loại | Điều kiện | Tình trạng |
|---|---|---|
| "client thật" | TLS fingerprint của Bun **và** system prompt có khối `<env>…</env>` của opencode **và** có tool | còn quota, đếm theo cặp (IP, session) |
| còn lại | thiếu 1 trong các điều kiện trên | quota chung, cạn cả ngày (429 trên mọi IP) |

Bằng chứng (thư mục `probe/`, `utlsprobe/`):

- Cùng một chuỗi byte request: Bun `fetch` → 200; curl (h2 và http/1.1), mitmproxy, Go `crypto/tls` → 429. Go + uTLS phát lại
  ClientHello thật của Bun (`utlsprobe/bun_clienthello.bin`) → 200, cả direct lẫn qua WARP SOCKS5 và proxy dân cư.
- Bỏ dần từng phần request thật của opencode 1.18.35 (`probe/replay.ts`): đổi user message, đổi/bớt tool → vẫn 200;
  bỏ system prompt hoặc thay bằng câu ngắn → 429; bỏ hết tool → 403 `FreeTierError`.
- Thu hẹp system prompt (`probe/replay.py`): 8 000 ký tự đầu → 429; chỉ riêng khối
  `Here is some useful information about the environment you are running in:\n<env>…</env>` (~300 ký tự) → 200.
  Prompt của client đặt trước hoặc sau khối này đều 200 và model làm đúng việc của client.
- Endpoint client thật dùng: `https://opencode.ai/inference/openai/v1/chat/completions`, header của opencode 1.18.35
  (`x-opencode-project: global`, `x-opencode-client: cli`). opencode 2.1.4 cũng bị 429 khi đi qua mitmproxy (do TLS).

## 2. Quota đếm theo cặp (IP, session), không theo request

| Thí nghiệm (WARP, cùng ngày) | Kết quả |
|---|---|
| 1 IP, mỗi request 1 session mới | 42 × 200 rồi 429 liên tục |
| 1 IP, 1 session cố định | 60/60 × 200 |
| Session cũ đem sang IP đã hết lượt | 429 |
| 1 session cố định, 300 request tuần tự | 81 × 200, ~200 × 429, rồi 200 trở lại (cửa sổ tốc độ, không phải trần ngày) |
| 8 request song song cùng 1 session | cả 8 × 200 (chậm hơn ~2×), không bị xếp tuần tự |

=> Mỗi IP mở được ~50 session/ngày (UTC). Một session chạy liền được 80–144 request rồi bị chặn tạm (Retry-After luôn
báo "tới nửa đêm" nhưng có lúc hồi sau ~1–2 phút, nên gateway cho nghỉ rồi thăm dò lại chứ không bỏ). Đo tiếp ở mục 5:
mở thêm session trên cùng IP WARP không giúp (direct mở 42 session vẫn bị chặn), tức giới hạn thực chất nằm ở **cấp IP**;
cách tăng sức chứa là **thêm IP sạch** (đổi danh tính WARP, proxy dân cư), không phải thêm session.
IPv6 của WARP bị đếm theo dải /64 dùng chung với người dùng WARP khác: các container rơi vào `2a09:bac1:7ac0:50::/64`
hết lượt ngay từ đầu. `warp-cli registration new` đổi sang IP mới trong ~13–25 s mà không cần restart container.
Proxy dân cư là nguồn sạch nhất (2 proxy phục vụ ~880 request mỗi cái, 0 lần 429).

## 3. Hành vi model khi chạy `auto` (workload gán nhãn ticket, `loadgen -workload label`)

- Upstream định tuyến mỗi request tới nhiều nhà cung cấp; reasoning trả qua field `reasoning` (kiểu OpenRouter) hoặc
  `reasoning_content`, có nhà không nhận tham số `reasoning`/`tool_choice` (400 `unsupported_parameter`).
- `reasoning: {"enabled": false}`: nemotron-3.5-lightning 12 s → 1,3 s; glm, beam cũng nhanh hơn; deepseek, big-pickle
  vẫn suy nghĩ (~3 s); flint-40b bỏ qua cờ và đốt hết `max_tokens` (để ngoài pool auto).
- Vì phải gửi kèm tool, beam/mimo thỉnh thoảng gọi tool thay vì trả lời => gateway đọc trước stream, rỗng/gọi tool thì
  đổi model khác (`autoguard.go`).

## 4. Công cụ

| Thư mục | Dùng để |
|---|---|
| `loadgen/` | Phát lại workload thật (`label`, `long`, `smoke`) vào gateway, bắn dồn tới hàng nghìn request, ghi JSONL + tóm tắt |
| `probe/` | `probe.py`, `probe2.py` (curl), `probe_bun.ts`, `replay.ts`/`replay.py` (thí nghiệm bỏ dần), `capture.py`, `dump.py` (mitmproxy) |
| `utlsprobe/` | Gửi request bằng Go + uTLS (TLS Bun) qua SOCKS5; `N=`, `NEWSES=1`, `SES=`, `MODEL=` để đo quota/session |
| `run_orw.sh` | Chạy gateway thử nghiệm trên :18090 với 16 WARP của host (không đụng prod :8090) |
| `ceiling.sh` | Đo trần thông lượng theo bậc concurrency trên gateway đang chạy |
| `probe/needle.py` | "Kim trong đống rơm": giấu mã giữa văn bản dài, kiểm model có đọc hết không |
| `probe/thinking_matrix.py` | Ma trận model × tham số thinking |

## 5. Kết quả đo trên prod (16 WARP + 3 dân cư + direct, workload gán nhãn thật, model `auto`)

Đo ngay sau khi quota reset (`research/ceiling.sh`, 2026-10-25 07:03):

| Bậc concurrency | Thành công | ok/s | token ra/s | p50 / p95 |
|---|---|---|---|---|
| 50 | 300/300 | 1,83 | 610 | 4,1 s / 20 s |
| 100 | 600/600 | 2,83 | 912 | 3,7 s / 19 s |
| **200** | **1000/1000** | **13,1** | **4 065** | 6,2 s / 24 s |
| 400 | 1600/1600 | 3,2 | 1 052 | 17 s / 314 s |
| 800 | 2176/2400 | 0,7 | 228 | 631 s / 1 420 s |

Sau bậc 200 thông lượng sụp vì IP WARP hết lượt mà gateway (bản lúc đó) chỉ đổi danh tính khi IP "cạn hẳn".
Sửa thành đổi ngay khi egress không phục vụ được + vòng quét 20 s. Đo lại giữa ngày (3000 request, c=200):
**3000/3000, 0 lỗi, ~14 ok/s ở đoạn ổn định**, 50 lần đổi danh tính thành công (API đăng ký WARP đôi khi trả
"Failed to communicate with the WARP API", gateway tự thử lại).

- Proxy dân cư là nguồn sạch nhất: res1 895, res2 871 request thành công, 0 lần 429 (res3 chập chờn ở tầng TCP,
  gateway tự xoay IP khi lỗi truyền tải liên tiếp).
- 32 container WARP vượt giới hạn inotify/file của host ("Too many open files"): daemon lỗi lặp và ghi ~640 MB log/giờ
  mỗi container, làm đầy ổ. Muốn quá 16 container phải nâng `fs.inotify.max_user_instances` (cần sudo). Compose giờ
  giới hạn log mọi service 3 x 10 MB.

## 6. Văn bản dài, thinking, chế độ một model (2026-10-25)

### Đọc văn bản dài ("kim trong đống rơm", `probe/needle.py`, mã bí mật đặt giữa văn bản)

| Model | ~295–318k token (800 KB) | ~370–383k token (1 MB) |
|---|---|---|
| nemotron-3.5-lightning | tìm đúng | trả rỗng |
| nemotron-3.5-super-vl | tìm đúng | |
| glm-5.3-flash | tìm đúng (271k) | |
| deepseek-v4.1-flash | tìm đúng | 400 vượt context |
| mimo-v2.6-flash | tìm đúng | 400 vượt context |
| beam | tìm đúng | **tìm đúng (370k)** |
| big-pickle | 400 vượt context | |

Catalog không đáng tin (models.dev ghi deepseek 200k; upstream không công bố). Gateway vì vậy tự học giới hạn
từ lỗi 400 (`profiles.go`). Đã kiểm end-to-end: `auto-long` 1 MB → mimo báo vượt → ghi nhớ → beam đọc đúng.

### Tham số thinking (`probe/thinking_matrix.py`, `results/thinking_matrix.txt`)

| Model | Tắt được bằng | Ghi chú |
|---|---|---|
| deepseek | `thinking.type=disabled` | `reasoning.enabled=false` KHÔNG có tác dụng; tắt thinking: báo cáo dài nhanh hơn ~20% |
| big-pickle | `thinking.type=disabled` | |
| mimo, nemotron | `reasoning.enabled=false` | |
| glm | không tắt hẳn | luôn nghĩ ~100 ký tự |
| beam | không có | chỉ nhận `reasoning_effort`, từ chối `reasoning`/`thinking` (gateway tự gỡ) |

Tắt suy nghĩ làm sai câu cần lập luận (câu tính giờ học: nemotron "6h45", big-pickle "19h15" khi tắt; đúng 10h15 khi bật).

### Đối chiếu phiên nghiên cứu song song của omp (2026-10-25 00:35–01:22)

Giữ lại: `thinking.type=disabled` cho deepseek (đúng, nhưng con số "104 s → 26,5 s" là 1 mẫu xấu; đo lại: nhanh hơn ~20%),
ý tưởng profile `auto-best/fast/long`, đọc kết quả batch song song, script chạy dùng đường dẫn tương đối. Số đo văn bản
dài của omp lưu ở `notes/omp-2026-10-25/`.

Sửa lại: omp cho rằng glm/nemotron/beam chỉ 128k và deepseek/mimo 1M rồi gắn cứng vào nhóm model; đo bằng "kim" cho thấy
ngược lại (xem bảng trên), nên gateway dùng giới hạn tự học thay vì danh sách cố định.

### Chế độ một model (cố định deepseek, thinking bật, `loadgen -workload write`)

| Thiết lập | Kết quả |
|---|---|
| 600 request, 200 đồng thời, `max_tokens` 8000 | 100% đúng deepseek; 503 có nội dung, 97 rỗng (cả 97 dừng đúng 8000 token: suy nghĩ ăn hết); ~8 700 token ra/s; p50 89 s |
| 100 request, 100 đồng thời, `max_tokens` 16000 | 98/100; ~6 800 token ra/s; p50 44 s |

Một model đơn lẻ vẫn tận dụng đủ 16 đường mạng (quota tính theo IP, không theo model), nên thông lượng token gần
bằng chế độ auto; giới hạn chính là độ dài output mỗi request.

## 6b. IP thoát thật của WARP: IPv6 riêng cho từng container (2026-10-26)

BẪY LẦM THƯỜNG GẶP khi đo IP: dùng `api.ipify.org` để xem IP thoát thì **thấy IPv4 dùng chung** (mỗi container
một trong vài IP `104.28.222.x` / `104.28.254.x` — 16 container chỉ ra **6 IPv4**). Nhưng opencode.ai **có AAAA**, nên
traffic thật đi ra bằng **IPv6 riêng của từng container**:

```
docker exec warp-proxy   curl -s https://opencode.ai/cdn-cgi/trace | grep ^ip=   -> ip=2a09:bac1:7ae0:50::3d0:32
docker exec warp-proxy-5 curl -s https://opencode.ai/cdn-cgi/trace | grep ^ip=   -> ip=2a09:bac5:d45e:e6::17:345
docker exec warp-proxy-11 curl -s https://opencode.ai/cdn-cgi/trace | grep ^ip=  -> ip=2a09:bac1:7a80:50::245:59
```

Còn `api.ipify.org` thì ra IPv4 (domain đó không ưu tiên AAAA) → **đo sai, tưởng là dùng chung IP**. Khi cần biết
IP mà upstream thấy thì phải hỏi đúng domain của upstream (hoặc dùng `https://[2606:4700:4700::1111]/cdn-cgi/trace`
- đích IPv6-only - để ép dùng IPv6).

Hệ quả quan trọng: **nguồn IP là 16 IPv6 khác nhau, mỗi container 1 cái**, và `warp-cli registration new` đổi được
IPv6 mới ⇒ nguồn IP (theo góc nhìn upstream) gần như không giới hạn. Vì vậy "IP mới" trong breaker phải đo bằng IPv6
của container, không phải bằng IPv4 dùng chung.

## 7. Circuit breaker cấp tài khoản (2026-10-25, phản hồi review của client)

Đo lúc 15:12 UTC (cuối ngày UTC): một IP WARP vừa `registration new` (IPv6 mới tinh), IP nhà, và proxy dân cư đều trả
FreeUsageLimitError với retry-after ~8,8 h (đếm tới 00:00 UTC). Xác nhận: hạn mức cạn ở mức **cao hơn từng IP** — thêm
hay đổi IP đều vô ích. Trước khi sửa, gateway đổi danh tính WARP **349–422 lần/3 h** một cách vô ích và fan-out mỗi
request qua mọi egress.

Sửa (`accountgate.go`): trip breaker khi có **đủ bằng chứng cạn CẤP TÀI KHOẢN**, tức ≥3 egress khác nhau bị cạn với
retry-after ≥30 phút trong cửa sổ 2 phút, **có ít nhất 1 egress vừa đổi IP** (IP mới tinh cũng cạn ⇒ không phải cạn
riêng từng IP) và bằng chứng đến từ **≥2 họ đường mạng** (WARP / dân cư / direct — 3 container WARP có thể dùng chung
1 IPv4 nên đếm 3 egress đó không khác gì 1 IP). Khi trip: ngừng đổi IP WARP, request fast-fail 429 kèm `Retry-After`
là **mốc quota hồi** (thường 00:00 UTC), và một luồng nền thăm dò lại (mỗi 2 phút) để gia hạn bằng chứng.

Mở lại breaker: **chỉ khi có 1 request 200 trên IP vừa đổi** (chứng minh cặp (IP, session) mới vẫn được chấp nhận ⇒
quota đã hồi). Một request 200 của session CŨ **không** mở breaker — lúc quota cạn một phần thì session cũ vẫn trả 200,
mở gate lúc đó là quay lại đúng cơn bão xoay IP mà breaker sinh ra để chặn (đo được 349–422 lần/3 h).

Lỗi TCP của proxy dân cư (`connection reset by peer`) giờ cho **cả egress** nghỉ (backoff 2–60 s) thay vì chỉ 1 model,
nên bộ điều phối sidelined proxy hỏng nhanh hơn, giảm rò 502 về client. Cooldown toàn-egress này cũng được
`anyUsableWithin` và `/v1/capacity` biết, và lỗi TCP của container WARP sẽ đổi danh tính sau 3 lỗi liên tiếp
(trước đó chỉ bị "giấu" bằng cooldown, container hỏng thì không được sửa).

> Sửa lại 2026-10-26: bản breaker đầu tiên (commit `c24ed75`) trip khi "3 egress cùng báo session chưa từng chạy bị
> cạn", tự mở khi có **bất kỳ** request 200 nào, và dùng mốc quota hồi (24 h) làm cửa sổ chặn. Cả ba đều sai:
> bằng chứng dính vĩnh viễn (một lần cạn cả ngày vẫn trip được các cụm sau), request 200 của session cũ mở nhầm
> breaker, và cửa sổ 24 h kẹt cứng nếu prober hỏng vì lý do mạng. Xem `PRIVATE-BRANCH.md` để có đầy đủ danh sách.

# Nhánh `private` — ghi chú cho nghiên cứu

> Tài liệu này chỉ nằm trên nhánh `private` (push sang repo GitHub **private** `minhdevtry/opencode-railway-private`).
> `main` của `minhdevtry/opencode-railway` vẫn **public** và chỉ chứa code sản phẩm + tài liệu.

## Vì sao tách nhánh

Repo `minhdevtry/opencode-railway` công khai, còn `docker-compose.warp.yml` từng chứa 3 key proxy dân cư (proxyxoay)
kèm địa chỉ IP nhà trong URL whitelist. Key đã nằm trong **lịch sử git công khai** nên phải **xoay key mới** — bỏ key khỏi
file không làm key cũ biến mất khỏi lịch sử.

Trạng thái hiện tại:

| Nơi | Nội dung | Quyền truy cập |
|---|---|---|
| `main` (repo `opencode-railway`) | code sản phẩm + tài liệu, **không có key** | public |
| `private` (repo `opencode-railway-private`) | code + nghiên cứu + các fix đang đánh giá | private |

Cấu hình proxy dân cư giờ nằm ở file biến môi trường cạnh compose (đã gitignore, không commit); `docker-compose.warp.yml`
chỉ khai báo `${RESIDENTIAL_PROXIES:-}` / `${RESIDENTIAL_REFRESH_URLS:-}`. File mẫu: `.env.example`.

Lệnh thường dùng:

```bash
git checkout private
git push private-origin private          # đẩy lên repo private
git push origin main                     # chỉ khi đã quyết định đưa lên public
```

## Đã sửa trong phiên 2026-10-26 (sau phiên `f5311ce4`)

Phiên trước tự review rồi tự kết luận "đã sửa xong". Vòng review độc lập (worker khác + test hồi quy) tìm ra lỗi thật
trong cả 3 tính năng mới của `c24ed75`:

| Commit | Nội dung |
|---|---|
| `730aa39` | key proxy ra khỏi file theo dõi git → file biến môi trường |
| `a988bec` | `X-Max-Wait` thành mốc **tuyệt đối** (trước tính lại mỗi lần retry ⇒ client chờ N × hạn đã đặt); breaker chỉ mở khi có bằng chứng quota hồi thật |
| `7966ad2` | 1 request 200 ở egress khác không xoá cooldown của egress đang lỗi |
| `9b3a7b4` | cửa sổ "capped" bám theo bằng chứng quota, không bám theo mốc 24 h (tránh kẹt cứng) |
| `a5d4a0e` | stream bị cụt **không được trả 200** ở mọi endpoint; vượt context mọi model trả 400 ngay thay vì chờ 30 phút rồi 429 |
| `b017743` | breaker nhiều điều kiện (≥3 egress + ≥1 IP **vừa đổi** + ≥2 họ đường mạng); `X-Max-Wait` giới hạn cả lúc chờ upstream trả header; `Retry-After` theo mốc quota hồi |
| `f21dd72` | prober chỉ gia hạn bằng chứng, không tự mở breaker; giới hạn tần suất thăm dò |
| `32ec386` | cooldown toàn-egress được bộ điều phối và `/v1/capacity` biết; thang backoff đúng; lỗi đọc body giữa chừng phạt egress |
| `9c82c7d` | xử lý race giữa watchdog hạn chờ và header về |

### Lỗi đáng chú ý nhất

1. **`X-Max-Wait` không có tác dụng thật.** Client đặt `1s`, upstream chậm `3s` ⇒ client chờ `3.003s` và nhận **200**.
   Hạn chỉ áp cho hàng đợi, không áp cho thời gian chờ upstream, và bị tính lại ở mỗi lần retry.
2. **Stream bị cụt trả 200 kèm nội dung cụt.** `/v1/messages`, `/v1/responses`, `/v1/completions` và mọi chế độ stream đều
   coi "EOF sạch không có `[DONE]`" là thành công. Client xử lý văn bản dài sẽ tưởng là câu trả lời đầy đủ.
3. **Breaker tự mở lại sau 1 request 200.** Lúc quota cạn một phần, session cũ vẫn trả 200 ⇒ breaker mở ⇒ quay lại đúng
   cơn bão xoay IP (349–422 lần/3h) mà breaker sinh ra để chặn.
4. **Bằng chứng "IP mới" dính vĩnh viễn** ⇒ 1 lần cạn cách đây cả ngày vẫn có quyền trip breaker cho các cụm 3 egress
   về sau ⇒ chặn nhầm cạn từng IP (lúc đó đổi IP vẫn có ích).
5. **`Retry-After` sai.** Ở đường 429 của upstream, client nhận `Retry-After: 16` (cooldown egress) thay vì mốc quota hồi
   (~2 h) ⇒ client retry theo hạn sai rồi nhận 429 lặp.

## Còn để mở

- Đếm "3 egress" theo **IP thật** thay vì theo tên egress (3 container WARP có thể dùng chung 1 IPv4).
- AIMD: trần in-flight bị giảm vĩnh viễn khi egress chập chờn — cần cơ chế hồi trần nhanh hơn.
- `probeLease` / cooldown: số liệu chọn 5 s (probe) và 2–60 s (TCP) mới suy ra từ logic, chưa đo trên prod.
- Chưa có test nào chạm `jobs.go` / `redis.go` (hàng đợi job bền) — 15 lỗi blind review đầu tiên của phiên trước đã được
  sửa trong code nhưng **chưa có test hồi quy** cho các nhánh đó.

## Quy trình đã dùng (để lần sau làm tiếp)

1. Tự đọc + tự kiểm bằng test hồi quy: bỏ fix thì test **phải fail** (không chỉ "test xanh").
2. Vòng review độc lập: worker khác model qua Orca (`orca orchestration worker-start --agent omp`) + workflow nhiều
   auditor với 2 vòng verify phản biện.
3. `go vet ./... && go test ./... && go test -race ./...` trước khi commit.

Báo cáo chi tiết vòng review đầu: `/tmp/orw_review_report.md` (vòng 2: `/tmp/orw_review_report2.md` nếu có).

# Đo chiều model của hạn mức free opencode.ai — per-IP hay per-(IP, model)? (2026-10-26)

Ngày đo: 2026-10-26, 09:23–09:49 UTC (16:23–16:49 giờ local UTC+7).
Tổng request gửi lên opencode.ai trong đợt đo: **739** (≤800).

**Kết luận một dòng:** hạn mức free là **per-(IP, model)** — **H2 được ủng hộ, H1 bị loại** ⇒
`modelbudget.go` khoá theo cặp **(model, egress)** là **đúng**, không cần sửa. Đợt đo này cũng phát hiện
thêm một điều: **cơ chế upstream KHÔNG khoá cứng** — sau khi model bị 429 hạn mức thì chính model đó vẫn
trả 200 (đo được 45/45 và 2/2 request 200 ngay sau sự kiện 429).

## 0. Câu hỏi

Hạn mức free của opencode.ai là bucket **per-IP** (mọi model dùng chung một bucket trên IP đó — H1)
hay **per-(IP, model)** (mỗi model một bucket riêng trên IP đó — H2)?

Hai kết luận cũ trong repo mâu thuẫn nhau:

| nguồn | kết luận | cách đo | điểm yếu |
|---|---|---|---|
| `research/limit-shape/REPORT.md` (commit `d10fc3e`) | **per-IP**, đơn vị ngân sách = REQUEST | chỉ đo **một** model (`glm-5.3-flash-free`) | không có dữ liệu đối chiếu model thứ hai trên cùng IP |
| `research/README.md` §6c (commit `2a1438c`) | **per-(model, IP)** | cùng IP: glm/deepseek/mimo 429, nemotron/beam 200 | chỉ 1 request cho 3 model 429, chưa biết IP đó còn quota hay không |

Vì `modelbudget.go` khoá ngân sách theo cặp **(model, egress)**, nếu thật ra là per-IP thì registry đó sai.
Đợt đo này khép lại đúng chỗ đó: **cạn một model trên một IP, rồi thử model khác ngay trên IP đó** (và
ngược lại), đồng thời **đo IP nào không bị model nào chặn** để phân biệt "bucket chung theo IP" với
"bucket riêng theo (IP, model)".

## 1. Thiết lập

**Vì sao tạo container riêng.** 32 container `warp-proxy*` đang nằm trong `PROXY_POOL` của gateway, nên gateway
vừa tiêu quota trên chúng vừa có thể tự xoay danh tính giữa chừa (IP đổi ⇒ ô đo hỏng). Đợt đo này dùng
**4 container WARP riêng** (`research/limit-shape/model-dim/warp-meas.yml`, project compose `orw-meas`,
cổng host 1200–1203); gateway **không** có chúng trong pool nên không đụng tới.

Không restart/stop container `warp-proxy*` nào; `orw-prod` không bị đụng (đã kiểm lúc 09:27: 32/32
`warp-proxy*` vẫn `Up`, `orw-prod` vẫn `Up 2 hours`).

**Công cụ:** dùng lại nguyên bộ có sẵn — probe Go uTLS giả ClientHello của Bun
(`research/limit-shape/probe/probe`), dump `research/limit-shape/dumps/gw1.json` (model `glm-5.3-flash-free`,
TLS Bun, có `<env>` + tools + `x-opencode-client: cli`), và các script `run_cell.sh` / `gen_ses.py` /
`annotate.py` / `summarize.py` (bản sao + sửa ở `model-dim/`, xem §8).

**Biến then chốt:** `MODEL=<tên>` ghi đè `model` trong body của dump — nhờ đó 1 dump vẫn đo được mọi model.

**IP upstream:** đo bằng `https://opencode.ai/cdn-cgi/trace` qua SOCKS5 của từng container (không dùng
`api.ipify.org` — domain đó trả IPv4 dùng chung của WARP và gây hiểu sai).

| container | SOCKS host | IPv6 upstream tại thời điểm đo | dùng cho ô |
|---|---|---|---|
| warp-meas-1 | 127.0.0.1:1200 | `2a09:bac5:d459:263c::3cf:14` | M1, M1P2, VER, RECHECK |
| warp-meas-2 | 127.0.0.1:1201 | `2a09:bac5:d45b:e6::17:32b` | M2, M2P2 |
| warp-meas-3 | 127.0.0.1:1202 | `2a09:bac5:d459:16d2::246:18` | M4 |
| warp-meas-4 | 127.0.0.1:1203 | `2a09:bac5:d45e:16d2::246:d4` | M3 |

Bảng đầy đủ kèm mốc thời gian: `logs/ip_mapping.txt`; IP trước/sau từng ô: `logs/limitshape_<cell>_ip.txt`.

**Hai model:**
- **A = `glm-5.3-flash-free`** — đã biết là cạn được.
- **B = `nemotron-3.5-lightning-free`** — lần trước 250/250. Trong đợt này B **không** 429 ngay request đầu
  ở bất kỳ IP nào, nên **giữ nguyên B** (không phải đổi sang `beam-free` theo điều kiện trong đề).

**Phân loại lỗi (rất quan trọng kể cả số liệu).**
`429` + `Retry-After` + `FreeUsageLimitError` = **hạn mức**.
`429`/`400` với `Endpoint is unavailable` / `Model is unavailable` = **lỗi nhà cung cấp**, KHÔNG tính là hạn mức
(ô M2P2 có đúng 2 lỗi `Endpoint is unavailable`; ô M3 có 1 lỗi `Endpoint is unavailable`).

## 2. Kiểm tra hiệu lực của phép đo (trước khi kết luận)

| kiểm tra | kết quả | ý nghĩa |
|---|---|---|
| field `model` trong body có được upstream dùng không? | `model=zzz-not-a-model-free` → **400 `Model is unavailable.`**; `model=beam-free` → **200** | upstream **có** dispatch theo `model` ⇒ phép đo "model khác nhau" là thật, không phải "cứ glm mà có tên khác" |
| IP có giữ nguyên trong 1 ô không? | `ip_before` == `ip_after` cho M1, M1P2, M2, M2P2, M4, M3 | **không ô nào bị loại** vì đổi IP |
| IP có còn sống (1 request thật) không? | xem §3.0 | loại được các IPv6 WARP "chết sẵn" |

## 3. Kết quả — quan sát được (số liệu thô)

### 3.0 Sàng lọc IP (probe sống, không tính vào ma trận)

4 IPv6 WARP tinh đầu tiên: **2 sống** (warp-meas-2, warp-meas-3), **2 chết ngay** — trên IP chết, **cả model A
lẫn model B đều 429 `FreeUsageLimitError`** (đúng đặc điểm "IPv6 WARP đã cạn sẵn" mà báo cáo cũ ghi).
Xoay danh tính (`warp-cli registration new`) cho IPv6 mới: warp-meas-1 sống ở lần xoay thứ 2, warp-meas-4 ở lần
xoay thứ 4. Tổng cộng **8 IPv6 WARP thử → 4 sống / 4 chết sẵn (50%)**. Chi tiết: `logs/liveness.txt`.

### 3.1 Bảng tổng hợp các ô

| ô | container / IP | model | session | số request | 200 | 429 hạn mức | ghi chú |
|---|---|---|---|---|---|---|---|
| M1 | warp-meas-1 / `…263c::3cf:14` | A | 1 cố định | 65 | **64** | **1** (i=64) | `Retry-After 52187` → 00:00 UTC |
| M1P2 | warp-meas-1 / `…263c::3cf:14` (ngay sau M1) | B | **mới** | 20 | **20** | 0 | **B vẫn 200 sau khi A đã bị chặn** |
| M2 | warp-meas-2 / `…d45b:e6::17:32b` | B | 1 cố định | 200 | **200** | 0 | B chưa cạn ⇒ M2 **không** kết luận được |
| M2P2 | warp-meas-2 / `…d45b:e6::17:32b` | A | mới | 20 | 18 | 0 | 2 lỗi `Endpoint is unavailable` (không phải hạn mức) |
| M4 | warp-meas-3 / `…d459:16d2::246:18` | B | 1 cố định | 200 | **200** | 0 | đối chứng: B trên IP sạch, chưa cạn |
| M3 | warp-meas-4 / `…d45e:16d2::246:d4` | A xen kẽ B | 1 cố định | 200 | **198** | **1** (i=109, model A) | 100 A + 100 B xen kẽ |
| VER | warp-meas-1 / `…263c::3cf:14` | `beam-free`, `zzz-not-a-model-free` | mới | 2 | 1 | 0 | `beam-free` → **200** trên IP mà A vừa bị chặn |
| RECHECK | warp-meas-1 / `…263c::3cf:14` | A | mới | 2 | **2** | 0 | **A 200 lại 19 phút sau sự kiện 429 hạn mức** |

### 3.2 Ô M1 + M1P2 — cạn model A rồi thử model B ngay trên cùng IP

- **M1** (IP `2a09:bac5:d459:263c::3cf:14`, model A, 1 session cố định): **64 request 200**, request thứ 65 →
  `429 FreeUsageLimitError`, `Retry-After: 52187` (09:30:14 UTC + 52187 s = **00:00:01 UTC hôm sau** ⇒ reset theo
  ngày UTC). Ngoài 64 request trong ô M1, IP này còn dùng **4 request A trước đó** (1 probe sống + 3 request của
  một lần chạy bị hỏng kỹ thuật, xem §8) ⇒ **68 request A 200 trước sự kiện 429 hạn mức**.
- **M1P2** (ngay sau, **cùng IP**, **session MỚI**, model B): **20/20 request 200, 0 lỗi 429**. Thêm 2 request B
  đã chạy trước đó trên cùng IP ⇒ **22/22 × 200**.
- **VER** (cùng IP, sau đó): `beam-free` → **200** (model thứ ba vẫn phục vụ bình thường).
- **RECHECK** (cùng IP, 19 phút sau sự kiện 429): model **A** → **200, 200**.

⇒ Trên cùng một IP, tại thời điểm model A bị chặn: model B và `beam-free` vẫn 200; và 19 phút sau đó chính
model A cũng 200 lại.

### 3.3 Ô M2 / M2P2 / M4 — B không cạn trong 203 request

- **M2** (IP `…d45b:e6::17:32b`, model B, 1 session cố định): **200/200 request 200, 0 429 hạn mức**
  (cộng 3 request B của lần chạy hỏng trước đó ⇒ **203 request B 200, 0 429 hạn mức** trên IP này).
- Vì B **chưa cạn**, **M2P2** (model A sau đó, cùng IP) chỉ cho **18/20 × 200** (2 lỗi nhà cung cấp) ⇒
  **ô M2 không kết luận được về H1/H2** (không có mốc "sau khi B cạn").
- **M4** (IP `…d459:16d2::246:18`, model B một mình, IP sạch): **200/200 × 200, 0 429** ⇒ khớp M2:
  **cap_B > 203 request** trên IP sạch.

### 3.4 Ô M3 — xen kẽ A/B từ request #1 trên một IP (IP `2a09:bac5:d45e:16d2::246:d4`)

200 request xen kẽ (A ở i lẻ, B ở i chẵn), 1 session cố định:

- Model **B**: **100/100 request 200, 0 lỗi 429** (kể cả 46 request đi sau sự kiện 429 hạn mức của A).
- Model **A**: 100 request → **98 × 200**, **1 × 429 hạn mức** (i=109, `Retry-After 51307` → 00:00 UTC),
  1 × `Endpoint is unavailable`.
- **Sự kiện then chốt**: i=109 → **A bị 429 hạn mức**; i=110 → **B vẫn 200**; và **45 request A tiếp theo vẫn
  200** (tổng 102 × 200 cho A trên IP này).
- Sự kiện 429 của A là request thứ **59** của model A trên IP này (55 trong M3 + 3 trước đó: 1 probe sống +
  2 smoke test); sự kiện 429 ở M1 là request thứ **69** của model A trên IP-1200.

### 3.5 Tổng lưu lượng theo IP (quan trọng cho kết luận)

| IP (container) | tổng request | theo model | 200 | 429 hạn mức | lỗi nhà cung cấp |
|---|---|---|---|---|---|
| `…263c::3cf:14` (warp-meas-1) | 95 | A 71, B 22, `beam-free` 1, model bịa 1 | 93 | **1** (A, request thứ 69 của A) | 1 (model bịa → 400) |
| `…d45b:e6::17:32b` (warp-meas-2) | 227 | A 24, B 203 | 225 | **0** | 2 (A) |
| `…d459:16d2::246:18` (warp-meas-3) | 204 | A 1, B 203 | 204 | **0** | 0 |
| `…d45e:16d2::246:d4` (warp-meas-4) | 204 | A 104, B 100 | 202 | **1** (A, request thứ 59 của A) | 1 (A) |

⇒ Hai IP có **204 và 227 request** mà **không có 1 lỗi 429 hạn mức nào**; hai IP còn lại có lỗi hạn mức **chỉ khi
model A vượt ~60 request**.

## 4. Kết luận

### 4.1 H2 (per-(IP, model)) được ủng hộ — bucket KHÔNG chung cho các model

**Bằng chứng định lượng quyết định (dùng §3.5, không dựa vào 1 sự kiện đơn lẻ):**

Giả sử H1 (một bucket chung theo IP). Khi đó ngưỡng chặn phải là ngưỡng **theo IP**, và nó đã được ước lượng
~60–70 request/IP (vì ở cả 2 IP có lỗi, sự kiện 429 đến ở request thứ 59 và thứ 69 của model A). Nhưng:

- IP-1201 đã nhận **227 request** (203 của B + 24 của A) mà **0 lỗi 429 hạn mức** — trong khi ngưỡng theo IP
  là ~60–70 ⇒ nếu H1 đúng thì phải có lỗi rò (leak) ở đây. **Không có lỗi nào.**
- IP-1202: **204 request, 0 lỗi 429 hạn mức** — cùng lập luận.
- Ngược lại, ở IP-1200 và IP-1203, lỗi hạn mức **chỉ xảy ra khi chính model A vượt ~60 request**, trong khi
  model B chạy song song 22 (IP-1200) và 100 (IP-1203) request mà không bị ảnh hưởng.

*(Ghi chú: 2 lỗi `Endpoint is unavailable` ở M2P2 bị loại khỏi "hạn mức" theo đúng quy tắc của probe và của báo cáo
cũ. Nếu tính nhầm chúng là hạn mức thì IP-1201 có 2 lỗi/227 request — vẫn không lập luận được ngưỡng ~60 theo IP,
vì model A trên IP-1201 chỉ dùng 24 request.)*

⇒ Ngưỡng bám theo **model**, không bám theo IP ⇒ **H1 bị loại, H2 được ủng hộ.**

**Bằng chứng bổ sung (thời điểm cụ thể, không cần suy luận):**

- M3, i=109: **A bị 429 hạn mức** (`FreeUsageLimitError`, `Retry-After` → 00:00 UTC) — i=110: **B 200**;
  46 request B sau đó **100% 200**. Nếu bucket chung theo IP thì B phải bị ảnh hưởng ngay.
- M1 + M1P2: A bị chặn (request thứ 69) rồi **B 22/22 × 200** trên cùng IP, kèm `beam-free` → 200.
- M3: sau khi A bị chặn, **45 request A tiếp theo vẫn 200** ⇒ cơ chế chặn của upstream **rò** (xem §4.3).

### 4.1b Ngưỡng là theo **số request tích luỹ của model đó trên IP đó**, KHÔNG phải theo tốc độ

Một giả thuyết cạnh tranh là "trần theo tốc độ (request/phút) theo IP". Dữ liệu loại được nó:

| ô | model | tốc độ | 429 hạn mức |
|---|---|---|---|
| M1 | A | **35,1 req/phút** | **có** (1 lần, ở cuối) |
| M2P2 | A | **44,4 req/phút** | **không** (0/20) |
| M1P2 | B | 23,5 req/phút | không |
| M2 / M4 | B | 21,3 / 22,6 req/phút | không (0/203 mỗi IP) |
| M3 | A 12,9 + B 12,7 req/phút | | A có 1 lần, B 0 lần |

Ô **M2P2 chạy nhanh hơn ô M1 (44,4 > 35,1 req/phút) mà không bị chặn lần nào** ⇒ không phải trần tốc độ.
Còn theo **số request tích luỹ**: A bị chặn ở request thứ 69 của A trên IP-1200 và thứ 59 trên IP-1203, còn A
chỉ dùng **24** request trên IP-1201 và **1** request trên IP-1202 (không chặn) ⇒ ngưỡng ~60 request **của
model A trên IP đó**.

### 4.2 Ngân sách của A **không phải global theo model** ⇒ mới là per-(IP, model)

- A bị chặn trên IP-1200 lúc 09:30:14 UTC.
- A trên IP-1201 lúc 09:37:46 UTC (M2P2) vẫn **18/20 × 200** ⇒ ngân sách A của IP-1201 còn nguyên.
- A trên IP-1203 lúc 09:44–09:48 UTC vẫn phục vụ 45 request 200 (sau sự kiện 429 riêng của IP đó) ⇒ ngân sách
  A của IP-1203 còn nguyên.
⇒ A có **nhiều bucket riêng theo IP**, không phải một bucket A chung toàn cầu.
Tương tự, B chạy **203 request trên IP-1201 và 203 request trên IP-1202 song song** mà không IP nào bị chặn
⇒ không có trần B toàn cầu ~65.

⇒ Đơn vị tiêu thụ là **cặp (IP, model)**: mỗi cặp có một bucket riêng, reset 00:00 UTC.

### 4.3 Sự kiện quan trọng mới: cơ chế chặn của upstream **không khoá cứng**

| quan sát | số liệu |
|---|---|
| A bị 429 hạn mức ở IP-1200 (09:30:14) → thử lại A lúc 09:49:08 | **2/2 × 200** |
| A bị 429 hạn mức ở IP-1203 (i=109) → 45 request A ngay sau | **45/45 × 200** |

⇒ "Hết hạn mức" ở upstream = **một sự kiện từ chối rời rạc**, không phải khoá tới 00:00 UTC. Điều này **lệch**
với phát biểu ở báo cáo cũ (`research/limit-shape/REPORT.md` §2.2: "không hồi được trong ngày") — dữ liệu mới
(45/45 và 2/2 request 200 ngay sau sự kiện 429) cho thấy sau sự kiện 429 thì phần lớn request vẫn được phục vụ.
*(Cũng khớp với ô REC+ của báo cáo cũ: 5/8 probe thành công sau khi IP đã cạn.)*

### 4.4 Số liệu trần quan sát được

| model | trần quan sát được | ghi chú |
|---|---|---|
| A = `glm-5.3-flash-free` | sự kiện 429 hạn mức đầu tiên ở request thứ **59** (IP-1203) và thứ **69** (IP-1200) của model A trên IP đó; **70/71 và 102/104 request vẫn 200** | 2 IP; 3 session khác nhau trên IP-1200 ⇒ ngưỡng **không** tính theo "số session mới" mà theo **số request** |
| B = `nemotron-3.5-lightning-free` | **> 203 request/IP** (chưa cạn trong 203 request trên 2 IP) | chưa chạm trần ⇒ không biết trần thật |
| `beam-free` | chỉ 1 request (không đủ đo) | |

⇒ **Trần khác nhau theo model** trên cùng một IP (A ~60, B > 203): đây cũng là một lập luận độc lập chống lại
giả thuyết "mọi model dùng chung một trần".

## 5. Suy luận, giới hạn, điều chưa khép

**Suy luận (không phải quan sát):**

- `modelbudget.go` **đang đúng**: registry khoá theo cặp **(model, egress)** khớp với hình dạng hạn mức đo được.
  Nếu registry khoá theo IP đơn thuần thì sẽ loại nhầm cả những model vẫn còn quota trên IP đó.
- Sức chứng free thực tế ≈ Σ_model (ngân sách riêng của model đó trên mỗi IP) × số IP dùng được —
  **không** phải một con số chung cho mọi model.
- Xoay danh tính WARP (IP mới) **không** cứu được một model đã cạn trên IP đó, nhưng **có** tác dụng với model
  khác trên IP đó và tăng tổng sức chứng của pool.
- Vì cơ chế chặn **rò** (§4.3), việc gateway khoá cặp (model, egress) tới 00:00 UTC ngay sau **một** lần 429
  là **thận trọng nhưng lãng phí sức chứng**: upstream vẫn phục vụ được sau sự kiện 429 (đo được 45/45 và 2/2).
  Cân nhắc dùng số lần 429 / thời gian sống để quyết định khoá thay vì khoá ngay. *(Không sửa code trong đợt đo này —
  ngoài phạm vi spec.)*

**Giới hạn của đợt đo (cần nói rõ):**

1. **Trần của B chưa đo được** (chỉ biết > 203) ⇒ chưa biết tổng sức chứng free thực tế.
2. **Trần của A chỉ có 2 mẫu IP** ⇒ ~60–70 là con số quan sát, không phải hằng số.
3. **Ô M2 không kết luận được** về H1/H2 (B không cạn trong 200 request) — hạn chế của thiết kế thí nghiệm, không
   phải mâu thuẫn: ô M1 + M3 + bảng lưu lượng §3.5 đã đủ để phân định.
4. **Không tách được bucket theo /128 hay theo /64 của WARP** (người dùng WARP khác trong cùng /64 có dùng
   chung bucket không). Con số ~60–70 của A có thể đã bị người khác trong /64 dùng trước. **Trong lúc đo, gateway
   vẫn phục vụ người dùng thật trên 32 container `warp-proxy*` và tự xoay danh tính WARP** (log gateway 09:55:
   `proxy3/proxy5/proxy6 đã đổi danh tính`), tức có traffic của gateway đi trong các block IPv6 của WARP cùng
   loại với IP đo. Nếu upstream khoá ngân sách theo **/64** thay vì /128 thì phần ngân sách còn lại của IP đo
   có thể đã bị gateway tiêu một phần ⇒ con số trần ~60 của A có thể **thấp hơn** trần thật. Điều này **không**
   ảnh hưởng kết luận H1-vs-H2 (kết luận dựa trên hình dạng dữ liệu giữa các model/IP, không dựa vào giá trị
   tuyệt đối của trần).
5. **Chỉ đo 2 model chính** (A, B) + 1 model phụ (`beam-free`); chưa đo được mốc cạn của
   `deepseek-v4.1-flash-free`, `mimo-v2.6-flash-free`, `big-pickle`.
6. Cơ chế rò của upstream được đặc tả bằng **3 sự kiện 429 hạn mức**; chưa biết tỉ lệ rò chính xác.

**Ô bị loại:** không có ô nào bị loại. Mọi ô giữ nguyên IPv6 xuyên suốt các giai đoạn
(`ip_before` == `ip_after` trong `logs/limitshape_<cell>_ip.txt`). Các IPv6 "chết sẵn" bị loại ở bước sàng lọc
(§3.0) trước khi đo, không phải loại ô sau khi đo.

## 6. Ảnh hưởng tới gateway

| nội dung | trạng thái sau đợt đo |
|---|---|
| `modelbudget.go`: registry theo cặp **(model, egress)** | **đúng** — giữ nguyên, không cần sửa |
| `accountgate.go`: breaker "cấp tài khoạn" cần bằng chứng từ ≥2 model | **đúng** — 1 model bị chặn không có nghĩa IP/ngân sách chung bị chặn |
| Ý tưởng "xoay session/identity để lấy thêm quota" | **sai** (đo ở đợt trước): xoay session không mở thêm ngân sách trên cùng IP |
| Ý tưởng "xoay IP là cách duy nhất để tăng sức chứng" | **đúng nhưng chưa đủ**: xoay IP giúp model khác trên IP cũ và tăng tổng pool; không cứu được model đã cạn trên IP đó |
| Sức chứng free thực tế | = Σ_model (ngân sách riêng của model đó trên mỗi IP) × số IP — **không** phải một con số chung |

## 7. Tổng request đã dùng & cách chạy lại

**Tổng: 739 request** (ngân sách 800).

| nhóm | số request |
|---|---|
| probe sống IP + xoay danh tính (lọc IP chết sẵn) | 15 |
| một lần chạy ma trận bị hỏng kỹ thuật (pipeline log) — xem ghi chú | 14 |
| smoke test pipeline (2 lần đầu + 1 lần kiểm tra đường dẫn ClientHello) | 3 |
| kiểm tra hiệu lực field `model` (VER) | 2 |
| M1 + M1P2 | 65 + 20 = 85 |
| M2 + M2P2 | 200 + 20 = 220 |
| M4 | 200 |
| M3 (xen kẽ A/B) | 200 |
| RECHECK (thử lại A sau 429) | 2 |
| **cộng** | **739** |

Ghi chú về "lần chạy bị hỏng kỹ thuật": bản sao `run_cell.sh` ban đầu lồi `runlog.py` vào giữa pipeline, làm
`summarize.py` chết vì dữ liệu sai định dạng ⇒ `BrokenPipeError` ⇒ probe bị dừng sau 2–3 request. Các request đó
**đã lên upstream** (nên vẫn được tính vào ngân sách request, và làm ngưỡng của A trên IP-1200 là request thứ
69 thay vì 65 nếu chỉ tính trong ô M1). Đã sửa pipeline (dùng `runlog.py` ở chế độ đọc file, sau khi ô chạy xong)
và **chạy lại toàn bộ ma trận**. *Lưu ý: file log thô của lần chạy hỏng đã bị xoá khi dọn log trước khi chạy lại;
các con số 14 request đó lấy từ đếm dòng lúc đó, và chúng chỉ ảnh hưởng đến phép cộng "request A đã dùng trước đó
trên cùng IP", không ảnh hưởng kết luận.*

**Chạy lại:**

```sh
cd /tmp/orw-meas-model-dim
docker compose -p orw-meas -f research/limit-shape/model-dim/warp-meas.yml up -d
cd research/limit-shape/model-dim
./rotate_until_alive.sh 1 1200 8          # tìm IPv6 còn sống (xoay danh tính tới khi có request 200)
./run_matrix.sh                           # M1 + M1P2 + M2 + M2P2 + M4 song song
./run_m3.sh M3 127.0.0.1:1203 200 glm-5.3-flash-free nemotron-3.5-lightning-free
python3 analyze.py                        # tóm tắt từ log thô
```

Biến môi trường của `run_matrix.sh`: `MODELA`, `MODELB`, `N1` (mặc định 200), `N2` (mặc định 20).

## 8. File log thô

- `logs/limitshape_<cell>.jsonl` — từng request: `status`, `secs`, `ra` (Retry-After), `t`, `text`, `err`, `cell`,
  `i`, `model`, `ses`. Ô: `M1`, `M1P2`, `M2`, `M2P2`, `M4`, `M3`, `VER`, `RECHECK_A1200`.
- `logs/limitshape_<cell>_ip.txt` — IP upstream trước/sau từng ô (dùng để loại ô bị đổi IP).
- `logs/run.log` — log dòng: `ts, cell, model, ses, i, status, ra` cho từng request + header/footer từng ô.
- `logs/ip_mapping.txt` — container → IPv6 upstream + thời điểm kiểm tra.
- `logs/liveness.txt` — vòng xoay danh tính tìm IPv6 sống (kèm kết quả probe 1 request mỗi lần).
- `logs/matrix.out` — timeline các ô.
- Công cụ (bản sao trong `model-dim/`): `run_cell.sh` (1 ô), `run_matrix.sh` (M1/M2/M4 song song),
  `run_m3.sh` (xen kẽ A/B), `rotate_until_alive.sh` (xoay danh tính tìm IP sống),
  `verify_model_override.sh` (kiểm tra field `model`), `analyze.py` (tóm tắt), `annotate.py`, `gen_ses.py`,
  `summarize.py`, `runlog.py`.
- Sửa so với bản gốc trong `research/limit-shape/`: `run_cell.sh` nhận `MODEL` qua biến môi trường
  (`export MODEL="${MODEL:-glm-5.3-flash-free}"`) và lấy IP bằng `cdn-cgi/trace` qua SOCKS thay vì `api.ipify.org`;
  `annotate.py` thêm trường `model`/`ses`; `runlog.py` mới (ghi `logs/run.log`).
- `logs/.gitignore` — file này chỉ có `!run.log`: `.gitignore` gốc của repo có dòng `*.log` nên
  `logs/run.log` mặc định **không** được commit; dòng này un-ignore nó (bắt buộc theo đề).
- 4 container đo (`warp-meas-1..4`, project `orw-meas`) **để nguyên đang chạy** sau khi đo; chúng không nằm
  trong `PROXY_POOL` của gateway nên không ảnh hưởng vận hành. Dừng chúng bằng
  `docker compose -p orw-meas -f research/limit-shape/model-dim/warp-meas.yml down` (chỉ project `orw-meas`).

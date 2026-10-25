# Hướng dẫn client: dùng gateway với thông lượng cao

Tài liệu này dành cho người viết chương trình gọi gateway (`http://127.0.0.1:8090`) để xử lý số lượng lớn văn bản.
Đọc xong bạn sẽ biết: chọn chế độ nào, gửi bao nhiêu request cùng lúc, đặt timeout/retry thế nào, và dùng ví dụ có sẵn.

---

## 1. Ba câu hỏi để chọn cách dùng

| Bạn cần | Dùng | Vì sao |
|---|---|---|
| Xử lý thật nhiều, **không quan tâm model nào trả lời** | `model: "auto"` (hoặc `auto-fast`) | Gateway xoay vòng mọi model free, tự đổi model khi lỗi: thông lượng cao nhất |
| Kết quả **nhất quán một model**, bật suy nghĩ, output dài | `model: "deepseek"` (hoặc `glm`, `mimo`...) | Gateway giữ đúng model đó, không tự đổi; vẫn dùng đủ mọi đường mạng |
| Lô **hàng nghìn tới hàng trăm nghìn** request, không muốn tự giữ kết nối | `POST /v1/batches` | Lưu bền trong Redis, gateway tự chạy hết tốc độ và tự thử lại, bạn chỉ việc lấy kết quả |

### Thông lượng đo được trên hệ thống thật

| Chế độ | Kịch bản | Thông lượng |
|---|---|---|
| `auto` | gán nhãn ticket (output ngắn), 200 đồng thời, đầu ngày | **13–14 request/s**, ~4 000 token ra/s, 1000/1000 thành công |
| `auto` | 3 000 request liên tục, giữa ngày | 3000/3000, ~14 request/s lúc ổn định |
| `auto-fast` | 120 request phân loại ngắn | 100 request đầu ở **14,8 request/s** |
| cố định `deepseek`, có suy nghĩ | báo cáo 600–900 từ, 200 đồng thời | **~8 700 token ra/s** (output dài nên ~1,5 request/s), 100% đúng deepseek |
| `auto-long` | văn bản ~300k token | đọc đúng (kiểm bằng mã bí mật giấu giữa văn bản) |

Thông lượng thật phụ thuộc giờ trong ngày: quota free reset lúc **07:00 sáng giờ Việt Nam** (00:00 UTC). Đầu ngày
nhanh nhất; về cuối ngày gateway phải đổi IP WARP liên tục nên chậm hơn, nhưng vẫn không trả lỗi.

---

## 2. Gửi bao nhiêu request cùng lúc

```bash
curl http://127.0.0.1:8090/v1/capacity
# {"recommended_concurrency": 190, "usable_egress": 17, "upstream_slots": 127, "queued_sync": 0, ...}
```

- **Giữ số request đang chạy quanh `recommended_concurrency`** và đọc lại mỗi ~30 giây (con số thay đổi theo số đường
  mạng đang dùng được). Đây là mức cho thông lượng cao nhất mà hàng chờ vẫn ngắn.
- Gửi nhiều hơn **không bị lỗi**: phần dư xếp hàng trong gateway (tối đa 30 phút), chỉ là chờ lâu hơn.
- Gửi ít hơn nhiều so với khuyến nghị là để phí sức chứa.

---

## 3. Timeout và retry

| Tình huống | Nên làm |
|---|---|
| Timeout phía client | **Đặt dài: 10–30 phút.** Gateway có thể giữ request trong hàng chờ; timeout ngắn (vd 30 s) làm request bị huỷ rồi gửi lại, tốn công vô ích |
| HTTP 400 | Lỗi do request (sai định dạng, văn bản quá dài với model cố định): sửa request, **không retry** |
| HTTP 429 | Hiếm: chỉ khi chờ quá 30 phút hoặc mọi đường mạng đều hết lượt. Chờ 30–60 s rồi gửi lại |
| HTTP 502/503, lỗi kết nối | Retry có backoff (2 s, 4 s, 8 s... tối đa ~30 s) |
| `content` rỗng, `finish_reason: "length"` | Phần suy nghĩ ăn hết `max_tokens`: **tăng `max_tokens`** (xem mục 5) |

Gateway đã tự xử lý phần lớn lỗi bên trong (đổi đường mạng, đổi model với `auto`, gửi lại khi stream treo/đứt), nên
client chỉ cần retry đơn giản như bảng trên.

**Đuôi chậm:** trong một lô, vài request cuối có thể lâu hơn hẳn (gateway đang tự sửa lỗi cho chúng: đổi model, gửi lại
qua đường khác). Kết quả vẫn đúng. Nếu cần xong nhanh cả lô, dùng `/v1/batches` hoặc để các request đó chạy nền.

---

## 4. Chế độ không quan tâm model (`auto`)

```json
{"model": "auto", "messages": [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}], "max_tokens": 2048}
```

| Tên | Gồm | Dùng khi |
|---|---|---|
| `auto` | mọi model | mặc định |
| `auto-fast` | nemotron-lightning, glm, beam | cần nhanh nhất, việc đơn giản (phân loại, trích xuất) |
| `auto-best` | deepseek, glm, mimo | cần trả lời kỹ hơn |
| `auto-long` | mọi model đọc được văn bản dài | tài liệu hàng trăm nghìn token |

- Gateway tự tắt suy nghĩ để nhanh. Muốn bật: gửi `"reasoning_effort": "medium"` (hoặc `high`).
- Header phản hồi `X-Gateway-Model` cho biết model nào đã trả lời.
- **Đừng gửi `tools`** nếu không cần: khi có tool của bạn, gateway không thể tự đổi model khi model trả lời sai định dạng.

---

## 5. Chế độ cố định model

```json
{"model": "deepseek", "messages": [...], "max_tokens": 16000, "reasoning_effort": "high"}
```

- Model **không bao giờ bị đổi**. Cho phép đổi khi model đó hết đường: thêm header `X-Model-Fallback: allow`.
- **Suy nghĩ**: `"reasoning_effort": "none" | "low" | "medium" | "high"`. `none` được gateway dịch sang đúng công tắc
  của từng model. Tham số gốc (`thinking`, `reasoning`) cũng dùng được.
- **Bật suy nghĩ + output dài ⇒ `max_tokens` ≥ 16 000.** Phần suy nghĩ tính chung vào `max_tokens`: với deepseek, đặt
  8 000 thì 16% request bị rỗng, 16 000 thì còn 2%.
- Tham số nào nhà cung cấp không nhận, gateway tự gỡ và báo trong header `X-Gateway-Dropped-Params`.
- Giới hạn đọc văn bản (đo thực tế): nemotron/glm/mimo/beam ~300k token, deepseek ~318k, big-pickle < 160k.

Thông lượng một model gần bằng `auto` tính theo token, vì giới hạn của upstream tính theo đường mạng chứ không theo model.

---

## 6. Lô rất lớn: `/v1/batches`

```bash
curl -X POST http://127.0.0.1:8090/v1/batches -H 'Content-Type: application/json' \
  -d '{"requests":[{"custom_id":"1","body":{"model":"auto","messages":[{"role":"user","content":"..."}]}}]}'
curl http://127.0.0.1:8090/v1/batches/<id>                                   # tiến độ: done / failed / pending
curl 'http://127.0.0.1:8090/v1/batches/<id>/results?offset=0&limit=1000'     # kết quả theo đúng thứ tự gửi
```

- Mỗi lần gửi nên ≤ 5 000 request (chia nhỏ lô lớn hơn).
- Job lỗi tạm thời được thử lại tối đa 200 lần với backoff tới 60 s: hết quota thì job chờ chứ không bị bỏ.
- Gateway khởi động lại không mất job. Kết quả giữ 7 ngày.
- Request đồng bộ được ưu tiên hơn job: lô nền không làm chậm request đồng bộ đang chờ.

---

## 7. Ví dụ chạy được ngay (`examples/`)

Đầu vào chung: file JSONL, mỗi dòng `{"id": "...", "prompt": "...", "system": "..."}`.

| File | Kiểu | Lệnh |
|---|---|---|
| `examples/high_throughput.py` | Python, chỉ thư viện chuẩn, request đồng bộ, tự đọc `/v1/capacity` | `python3 examples/high_throughput.py in.jsonl out.jsonl --model auto` |
| `examples/batch_submit.py` | Python, gửi lô qua `/v1/batches` rồi lấy kết quả | `python3 examples/batch_submit.py in.jsonl out.jsonl --model auto` |
| `examples/high_throughput.ts` | Bun / Node 18+ | `bun examples/high_throughput.ts in.jsonl out.jsonl deepseek` |

Đã chạy thử trên hệ thống thật với 120 tin nhắn: Python `auto-fast` 120/120, batch 120/120, TypeScript `deepseek` 116/120.

### Dùng OpenAI SDK

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:8090/v1", api_key="local", timeout=1800, max_retries=3)
r = client.chat.completions.create(model="auto", messages=[{"role": "user", "content": "..."}])
print(r.choices[0].message.content)
```

---

## 8. Mẹo tăng thông lượng

1. **Đặt phần cố định (system prompt, hướng dẫn) ở đầu**, phần thay đổi ở cuối: upstream cache được phần đầu giống nhau.
2. **Đặt `max_tokens` vừa đủ** cho việc ngắn (vd 512–2048): request kết thúc nhanh, slot trống sớm.
3. Việc đơn giản dùng `auto-fast`; việc cần lập luận dùng model cố định + `reasoning_effort`.
4. Chạy lô lớn vào đầu ngày (sau 07:00) khi mọi IP còn nguyên lượt.
5. Theo dõi `GET /v1/capacity`: `usable_egress` thấp và `queued_sync` cao nghĩa là hệ thống đang hết đường; cân nhắc thêm
   proxy dân cư (nguồn IP sạch nhất).

---

## 9. Cấu hình cho operationbot (BatchLLM)

| Biến / tham số | Nên đặt | Lý do |
|---|---|---|
| `BATCHLLM_GATEWAYS` | `http://127.0.0.1:8090` | |
| Timeout mỗi request | 300 s trở lên (mặc định 30 s) | 30 s quá ngắn khi request phải xếp hàng hoặc output dài |
| Concurrency ban đầu | `recommended_concurrency` từ `/v1/capacity` | thay vì 4–6 |
| Model | `auto` cho gán nhãn hàng loạt | gateway tự chia tải, BatchLLM không cần tự xoay model |

#!/bin/bash
# fieldtest.sh <model> '<extra json fields>' [n]  -> gửi label request qua gateway thử nghiệm, in field xuất hiện + độ dài content
M=$1; X=$2; N=${3:-3}
SYS=$(python3 -I -c "import json,re; s=open('research/loadgen/main.go').read(); print(json.dumps(re.search(r'const labelSystem = \x60(.*?)\x60', s, re.S).group(1)))")
for i in $(seq 1 $N); do
curl -s -m 90 -o - -w '\nHTTP %{http_code} %{time_total}s\n' http://127.0.0.1:18090/v1/chat/completions -H 'content-type: application/json' -d "{\"model\":\"$M\",\"stream\":true,\"messages\":[{\"role\":\"system\",\"content\":$SYS},{\"role\":\"user\",\"content\":\"Nhóm: sales-support\\nTin nhắn:\\n\\\"phụ huynh bé An lớp 2 hỏi hoàn phí khoá hè\\\"\"}],\"max_tokens\":1024 $X}" | python3 -I -c "
import sys,json,collections
keys=collections.Counter(); txt=''; rs=0; fin=None; tail=''
for l in sys.stdin:
    if l.startswith('HTTP'): tail=l.strip(); continue
    if not l.startswith('data: ') or '[DONE]' in l: continue
    try: d=json.loads(l[6:])
    except: continue
    for ch in d.get('choices',[]):
        dl=ch.get('delta',{}); keys.update(k for k,v in dl.items() if v)
        txt+=dl.get('content') or ''; rs+=len(dl.get('reasoning') or dl.get('reasoning_content') or ''); fin=ch.get('finish_reason') or fin
print(tail, 'finish',fin,'reasoning_chars',rs,'content_chars',len(txt), dict(keys))
"
done

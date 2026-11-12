#!/bin/bash
# Watchdog cho cụm Cloudflare WARP (docker-compose.warp.yml).
#
# Vì sao cần: container WARP hay khởi động lỗi — /etc/resolv.conf bị
# cloudflare-warp ghi đè thành nameserver 127.0.2.2/127.0.2.3 nhưng DNS proxy
# nội bộ WARP không chạy → mọi request ra ngoài fail với
# "socks5: connect failed, status 0x04" (host unreachable) hoặc
# "connection refused" khi phân giải DNS. Docker healthcheck báo unhealthy
# nhưng `restart: unless-stopped` KHÔNG restart container chỉ vì unhealthy
# (chỉ restart khi process thoát) → container chết âm thầm, gateway quay
# vòng pool gặp proxy chết và trả 502.
#
# Cách sửa: mỗi lần chạy, restart container đang unhealthy. Container WARP
# khởi động lại sẽ tạo lại tunnel + DNS proxy và trở lại healthy.
set -u

# Chỉ quản lý 4 egress WARP đang dùng; warp-proxy-5..32 đã nghỉ.
# Override bằng WARP_WATCH_CONTAINERS nếu cần đổi danh sách.
WARP_CONTAINERS="${WARP_WATCH_CONTAINERS:-warp-proxy warp-proxy-2 warp-proxy-3 warp-proxy-4}"
CONTAINERS=""
for c in $WARP_CONTAINERS; do
    docker inspect "$c" >/dev/null 2>&1 && CONTAINERS="$CONTAINERS $c"
done
[ -z "$CONTAINERS" ] && exit 0

fixed=0
for c in $CONTAINERS; do
    state=$(docker inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$c" 2>/dev/null)
    status="${state%%|*}"
    health="${state##*|}"

    if [ "$status" != "running" ]; then
        echo "[warp-watch] $c status=$status -> start"
        docker start "$c" >/dev/null 2>&1 && fixed=$((fixed + 1))
        continue
    fi

    case "$health" in
        healthy) continue ;;
        starting) continue ;;
    esac
    echo "[warp-watch] $c health=$health -> restart"
    docker restart "$c" >/dev/null 2>&1 && fixed=$((fixed + 1))
    sleep 5
done

[ "$fixed" -gt 0 ] && echo "[warp-watch] restarted $fixed container(s)"
exit 0

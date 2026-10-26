package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config gom toàn bộ cấu hình đọc từ biến môi trường.
type Config struct {
	Port           string
	MaxConcurrency int
	MaxBodyBytes   int64

	// Định tuyến egress
	MaxWait            time.Duration // tổng thời gian tối đa 1 request chờ egress/cooldown
	MaxAttempts        int
	SessionIDMode      string          // client (mặc định: ID ổn định theo hội thoại của client) | fresh (mỗi request 1 ID) | stable (1 ID cho cả tiến trình)
	ProjectIDMode      string          // như trên, cho project/device ID
	ImageModels        map[string]bool // model nhận được ảnh (image_url); các model khác ảnh bị hạ thành văn bản
	StreamIdle         time.Duration   // hủy request nếu upstream không gửi byte nào trong khoảng này
	CooldownMax        time.Duration   // trần cooldown cho egress không rotate được (direct, proxy tĩnh), kể cả khi Retry-After báo lâu hơn (IP direct có thể đổi)
	TorIdleStop        time.Duration   // Tor không có request quá lâu thì tắt để nhường RAM
	TorMaxInflight     int             // trần request đồng thời mỗi Tor (vượt thì bật thêm Tor hoặc chờ)
	DirectWarnReqs     int             // số request direct cho 1 model trong 2 phút được coi là "gần ngưỡng nguy hiểm" => bật Tor sẵn
	DirectWarnInflight int             // số request direct đang chạy cùng lúc được coi là nguy hiểm
	AutoModelFallback  bool            // tự động failover sang model khác trong freeModelPool khi model yêu cầu cạn quota trên mọi egress
	DirectCooldown     time.Duration   // thời gian direct nghỉ khi bị 429 (mặc định 2m: cửa sổ trượt upstream); trong thời gian này request vào thẳng Tor
	QueueWait          time.Duration   // chờ slot tối đa trước khi trả 503
	TorBackoffMax      time.Duration   // trần backoff của Tor sau chuỗi 429 liên tiếp (exit cạn quota thì đừng đập thêm)
	TorRotateCooldown  time.Duration   // nghỉ ngắn của 1 Tor instance sau khi bị 429 (đã đổi circuit)
	DisableTor         bool
	ForceTor           bool          // không dùng direct (để đo/so sánh)
	TorExitNodes       string        // vd "{sg},{jp}": giới hạn quốc gia exit châu Á
	TorExitNodesEU     string        // vd "{de},{nl}": giới hạn quốc gia exit Tây Âu
	TorIdentities      int           // số identity (circuit/exit IP riêng) trên mỗi daemon Tor
	PreferDirect       bool          // true: direct trước, Tor khi 429; false (mặc định): Tor trước, direct dự phòng
	TorWarm            int           // số Tor chạy sẵn lúc khởi động (dự phòng nóng)
	TtfbRetry          time.Duration // Tor: quá thời gian này chưa có header thì bỏ circuit đó và thử circuit khác
	TorLoad            int           // số request đồng thời trung bình mỗi daemon Tor, vượt thì bật thêm daemon (daemon Tor đơn luồng, CPU là giới hạn)
	TorMax             int           // số Tor tối đa được bật thêm khi bị 429
	TorBin             string

	// Xác thực & giới hạn theo IP
	AllowedIPs     string
	ClientIPHeader string
	RateRPM        int
	RateBurst      int
	CORSOrigin     string
	ProxyRefreshURL      string        // URL gọi để đổi IP cho proxy xoay khi bị 429 (vd: https://proxyxoay.shop/api/get.php?key=...)
	ProxyWarmup          time.Duration // thời gian chờ proxy xoay IP xong (mặc định 3s)
	ResidentialProxies     string
	ResidentialRefreshURLs string
	AutoModels             string

	// Giả danh client opencode thật (cổng phân loại free tier của upstream, đo 2026-10-24, xem research/README.md)
	UpstreamTLS   string // bun (mặc định: uTLS giả ClientHello của Bun) | go (crypto/tls, bị dồn vào quota chung)
	UpstreamBase  string // endpoint chat của client thật
	ClientProfile string // bộ header: 1.18 (opencode 1.18.35, đã đo qua) | 2.1 (opencode 2.1.4)
	EnvBlock      bool   // ghép khối <env> của opencode vào system prompt (thiếu khối này => 429 FreeUsageLimitError)
	SessionsPerEgress int           // trần số session SỐNG CÙNG LẤP trên mỗi egress (SESSION_ID_MODE=egress); upstream cho ~50 session/IP/ngày
	SessionInflight   int           // số request đồng thời trên 1 session trước khi mở session mới
	SessionCoolMax    time.Duration // Retry-After tối đa được tin để cho session nghỉ (dài hơn => nghỉ SessionReprobe rồi thử lại)
	SessionReprobe    time.Duration // session/IP bị chặn với Retry-After dài: thử lại sau khoảng này
	WarpContainers    string // tên container WARP theo đúng thứ tự PROXY_POOL (trống => warp-proxy, warp-proxy-2, ...)
	WarpRenewParallel int    // số container được đổi danh tính cùng lúc

	AutoGuard   bool // model auto: tool_choice=none, tắt reasoning ở model hỗ trợ, đổi model khi kết quả rỗng/gọi tool
	AutoRetries int
	AutoFirstToken time.Duration // auto: quá hạn này chưa có token đầu thì bỏ, thử egress/model khác
	AutoFastModels, AutoBestModels, AutoLongModels string // ghi đè danh sách model của auto-fast/best/long

	// Trần in-flight thích ứng mỗi egress (AIMD)
	LimitWarp, LimitRes, LimitDirect, LimitMin, LimitMax int

	// Hàng đợi job bền
	RedisURL       string
	JobWorkers     int
	JobMaxAttempts int
	JobMaxBackoff  time.Duration

	// /v1/messages: bơm 4 tool stub của opencode (bash/glob/grep/read). Upstream (FreeTier gate) bắt buộc có.
	DefaultModel         string
	InjectToolsAnthropic bool
}

func envStr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// envInt đọc số nguyên >= min; sai định dạng hoặc nhỏ hơn min thì dùng def.
func envInt(key string, def, min int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min {
		log.Printf("[Config] %s=%q không hợp lệ, dùng mặc định %d", key, v, def)
		return def
	}
	return n
}

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func envBoolDef(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func envDur(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d >= 0 {
		return d
	}
	log.Printf("[Config] %s=%q không hợp lệ, dùng mặc định %v", key, v, def)
	return def
}

func loadConfig() Config {
	torMax := envInt("TOR_MAX", envInt("TOR_COUNT", 10, 0), 0)
	c := Config{
		Port:                 envStr("PORT", defaultPort),
		MaxConcurrency:       envInt("MAX_CONCURRENCY", 1000, 1),
		MaxBodyBytes:         int64(envInt("MAX_BODY_MB", 32, 1)) << 20,
		MaxWait:              envDur("MAX_WAIT", 300*time.Second),
		MaxAttempts:          envInt("MAX_ATTEMPTS", 30, 1),
		StreamIdle:           envDur("STREAM_IDLE_TIMEOUT", 60*time.Second),
		SessionIDMode:        strings.ToLower(envStr("SESSION_ID_MODE", "egress")),
		ProjectIDMode:        strings.ToLower(envStr("PROJECT_ID_MODE", "client")),
		ImageModels:          map[string]bool{},
		CooldownMax:          envDur("COOLDOWN_MAX", 2*time.Minute),
		TorRotateCooldown:    envDur("TOR_ROTATE_COOLDOWN", time.Second),
		TorBackoffMax:        envDur("TOR_BACKOFF_MAX", 2*time.Minute),
		TorIdleStop:          envDur("TOR_IDLE_STOP", 30*time.Minute),
		TorMaxInflight:       envInt("TOR_MAX_INFLIGHT", 24, 1),
		DirectWarnReqs:       envInt("DIRECT_WARN_REQS", 150, 1),
		DirectWarnInflight:   envInt("DIRECT_WARN_INFLIGHT", 60, 1),
		DirectCooldown:       envDur("DIRECT_COOLDOWN", 2*time.Minute),
		QueueWait:            envDur("QUEUE_WAIT", 30*time.Second),
		AutoModelFallback:    envBool("AUTO_MODEL_FALLBACK"),
		DisableTor:           envBool("DISABLE_TOR"),
		ForceTor:             envBool("FORCE_TOR"),
		TorMax:               torMax,
		TorLoad:              envInt("TOR_LOAD", 25, 1),
		TtfbRetry:            envDur("TTFB_RETRY", 15*time.Second),
		TorBin:               envStr("TOR_BIN", "tor"),
		AllowedIPs:           strings.TrimSpace(os.Getenv("ALLOWED_IPS")),
		ClientIPHeader:       envStr("CLIENT_IP_HEADER", ""),
		RateRPM:              envInt("RATE_LIMIT_RPM", 6000, 0),
		RateBurst:            envInt("RATE_LIMIT_BURST", 600, 1),
		CORSOrigin:           strings.TrimSpace(os.Getenv("CORS_ORIGIN")),
		DefaultModel:         envStr("DEFAULT_MODEL", "deepseek"),
		ProxyRefreshURL:      envStr("PROXY_REFRESH_URL", ""),
		ProxyWarmup:          envDur("PROXY_WARMUP", 3*time.Second),
		InjectToolsAnthropic: envBoolDef("INJECT_TOOLS_ANTHROPIC", true),
		ResidentialProxies:     envStr("RESIDENTIAL_PROXIES", ""),
		ResidentialRefreshURLs: envStr("RESIDENTIAL_REFRESH_URLS", ""),
		AutoModels:             envStr("AUTO_MODELS", ""),
		UpstreamTLS:            strings.ToLower(envStr("UPSTREAM_TLS", "bun")),
		UpstreamBase:           strings.TrimRight(envStr("UPSTREAM_BASE", "https://opencode.ai/inference/openai/v1"), "/"),
		ClientProfile:          envStr("CLIENT_PROFILE", "1.18"),
		EnvBlock:               envBoolDef("ENV_BLOCK", true),
		SessionsPerEgress:      envInt("SESSIONS_PER_EGRESS", 8, 1),
		SessionInflight:        envInt("SESSION_INFLIGHT", 4, 1),
		SessionCoolMax:         envDur("SESSION_COOL_MAX", 30*time.Minute),
		SessionReprobe:         envDur("SESSION_REPROBE", 10*time.Minute),
		WarpContainers:         envStr("WARP_CONTAINERS", ""),
		WarpRenewParallel:      envInt("WARP_RENEW_PARALLEL", 2, 1),
		AutoGuard:              envBoolDef("AUTO_GUARD", true),
		AutoRetries:            envInt("AUTO_RETRIES", 3, 0),
		AutoFirstToken:         envDur("AUTO_FIRST_TOKEN", 25*time.Second),
		AutoFastModels:         envStr("AUTO_FAST_MODELS", ""),
		AutoBestModels:         envStr("AUTO_BEST_MODELS", ""),
		AutoLongModels:         envStr("AUTO_LONG_MODELS", ""),
		LimitWarp:              envInt("LIMIT_WARP", 4, 1),
		LimitRes:               envInt("LIMIT_RES", 4, 1),
		LimitDirect:            envInt("LIMIT_DIRECT", 2, 1),
		LimitMin:               envInt("LIMIT_MIN", 1, 1),
		LimitMax:               envInt("LIMIT_MAX", 32, 1),
		RedisURL:               envStr("REDIS_URL", ""),
		JobWorkers:             envInt("JOB_WORKERS", 256, 1),
		JobMaxAttempts:         envInt("JOB_MAX_ATTEMPTS", 200, 1),
		JobMaxBackoff:          envDur("JOB_MAX_BACKOFF", 60*time.Second),
	}
	for _, m := range splitList(os.Getenv("IMAGE_MODELS")) {
		c.ImageModels[strings.ToLower(m)] = true
	}
	c.TorExitNodes = envStr("TOR_EXIT_NODES", "{sg},{jp},{hk},{kr}")
	c.TorExitNodesEU = envStr("TOR_EXIT_NODES_EU", "{de},{nl},{fr},{gb},{ch}")
	c.TorIdentities = envInt("TOR_IDENTITIES", 16, 1)
	c.PreferDirect = envBool("PREFER_DIRECT")
	c.TorWarm = envInt("TOR_WARM", 6, 0)
	if c.TorWarm > c.TorMax {
		c.TorWarm = c.TorMax
	}
	// Trên Railway, IP client thật nằm trong header do edge gắn vào, RemoteAddr chỉ là proxy nội bộ.
	if c.ClientIPHeader == "" && os.Getenv("RAILWAY_ENVIRONMENT") != "" {
		c.ClientIPHeader = "X-Real-IP"
	}
	return c
}

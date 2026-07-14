package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	DefaultPublicBaseURL    = "https://api.luminet.cn/hifumi/"
	DefaultFrontendOrigins  = "https://stellafortuna.hifumi.luminet.cn,https://stellafortuna.luminet.cn"
	DefaultFrontendReturn   = "https://stellafortuna.luminet.cn/"
	DefaultRedisKeyPrefix   = "study-list:prod:"
	DefaultHTTPAddr         = ":8080"
	DefaultSessionAudience  = "stellafortuna"
	MinimumSigningSecretLen = 32
)

type Config struct {
	HTTPAddr          string
	PublicBaseURL     *url.URL
	FrontendOrigins   []string
	FrontendReturnURL *url.URL
	MySQLDSN          string
	RedisURL          string
	RedisKeyPrefix    string
	LinuxDOClientID   string
	LinuxDOSecret     string
	SessionSecret     string
	SessionAudience   string
	TrustedProxyCIDRs []string
	LogLevel          string
	ShutdownTimeout   time.Duration
}

type Lookup func(string) (string, bool)

func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

func LoadFrom(lookup Lookup) (Config, error) {
	value := func(name, fallback string) string {
		if raw, ok := lookup(name); ok && strings.TrimSpace(raw) != "" {
			return strings.TrimSpace(raw)
		}
		return fallback
	}

	publicBase, err := normalizedBaseURL(value("PUBLIC_BASE_URL", DefaultPublicBaseURL))
	if err != nil {
		return Config{}, fmt.Errorf("PUBLIC_BASE_URL: %w", err)
	}
	frontendOrigins, err := normalizedOrigins(value("FRONTEND_ORIGINS", DefaultFrontendOrigins))
	if err != nil {
		return Config{}, fmt.Errorf("FRONTEND_ORIGINS: %w", err)
	}
	frontendReturn, err := url.Parse(value("FRONTEND_RETURN_URL", DefaultFrontendReturn))
	if err != nil || frontendReturn.Scheme != "https" || frontendReturn.Host == "" || frontendReturn.User != nil {
		return Config{}, fmt.Errorf("FRONTEND_RETURN_URL must be an absolute https URL")
	}
	if !contains(frontendOrigins, frontendReturn.Scheme+"://"+frontendReturn.Host) {
		return Config{}, fmt.Errorf("FRONTEND_RETURN_URL origin must be listed in FRONTEND_ORIGINS")
	}

	cfg := Config{
		HTTPAddr:          value("HTTP_ADDR", DefaultHTTPAddr),
		PublicBaseURL:     publicBase,
		FrontendOrigins:   frontendOrigins,
		FrontendReturnURL: frontendReturn,
		MySQLDSN:          value("MYSQL_DSN", ""),
		RedisURL:          value("REDIS_URL", ""),
		RedisKeyPrefix:    value("REDIS_KEY_PREFIX", DefaultRedisKeyPrefix),
		LinuxDOClientID:   value("LINUXDO_CLIENT_ID", ""),
		LinuxDOSecret:     value("LINUXDO_CLIENT_SECRET", ""),
		SessionSecret:     value("SESSION_JWT_SECRET", ""),
		SessionAudience:   value("SESSION_AUDIENCE", DefaultSessionAudience),
		TrustedProxyCIDRs: splitCSV(value("TRUSTED_PROXY_CIDRS", "")),
		LogLevel:          strings.ToLower(value("LOG_LEVEL", "info")),
		ShutdownTimeout:   30 * time.Second,
	}
	return cfg, nil
}

func (c Config) ValidateServe() error {
	required := map[string]string{
		"MYSQL_DSN":             c.MySQLDSN,
		"REDIS_URL":             c.RedisURL,
		"LINUXDO_CLIENT_ID":     c.LinuxDOClientID,
		"LINUXDO_CLIENT_SECRET": c.LinuxDOSecret,
		"SESSION_JWT_SECRET":    c.SessionSecret,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if len(c.SessionSecret) < MinimumSigningSecretLen {
		return fmt.Errorf("SESSION_JWT_SECRET must contain at least %d characters", MinimumSigningSecretLen)
	}
	for _, value := range c.TrustedProxyCIDRs {
		if _, _, err := net.ParseCIDR(value); err != nil {
			return fmt.Errorf("TRUSTED_PROXY_CIDRS contains invalid CIDR %q", value)
		}
	}
	return nil
}

func (c Config) ValidateMigrate() error {
	if strings.TrimSpace(c.MySQLDSN) == "" {
		return fmt.Errorf("MYSQL_DSN is required")
	}
	return nil
}

func (c Config) PublicIssuer() string {
	return strings.TrimSuffix(c.PublicBaseURL.String(), "/")
}

func (c Config) PublicPath(relative string) string {
	base := strings.TrimSuffix(c.PublicBaseURL.Path, "/")
	return base + "/" + strings.TrimPrefix(relative, "/")
}

func (c Config) PublicURL(relative string) string {
	ref := &url.URL{Path: strings.TrimPrefix(relative, "/")}
	return c.PublicBaseURL.ResolveReference(ref).String()
}

func normalizedBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("must be an absolute https URL without query or fragment")
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u, nil
}

func normalizedOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("must be an https origin without a path")
	}
	return u.Scheme + "://" + u.Host, nil
}

func normalizedOrigins(raw string) ([]string, error) {
	items := splitCSV(raw)
	if len(items) == 0 {
		return nil, fmt.Errorf("must contain at least one https origin")
	}
	origins := make([]string, 0, len(items))
	for _, item := range items {
		origin, err := normalizedOrigin(item)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", item, err)
		}
		origins = appendUnique(origins, origin)
	}
	return origins, nil
}

func appendUnique(values []string, value string) []string {
	if contains(values, value) {
		return values
	}
	return append(values, value)
}

func contains(values []string, value string) bool {
	for _, existing := range values {
		if existing == value {
			return true
		}
	}
	return false
}

func splitCSV(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

package config

import "testing"

func TestLoadDefaultsPreserveHifumiPrefix(t *testing.T) {
	cfg, err := LoadFrom(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.PublicURL("api/v1/auth/session"); got != "https://api.luminet.cn/hifumi/api/v1/auth/session" {
		t.Fatalf("unexpected public URL: %s", got)
	}
	if got := cfg.PublicPath("healthz"); got != "/hifumi/healthz" {
		t.Fatalf("unexpected public path: %s", got)
	}
}

func TestValidateServeRejectsMissingSecrets(t *testing.T) {
	cfg, err := LoadFrom(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateServe(); err == nil {
		t.Fatal("expected missing configuration to be rejected")
	}
}

func TestLoadRejectsFrontendPath(t *testing.T) {
	_, err := LoadFrom(func(name string) (string, bool) {
		if name == "FRONTEND_ORIGIN" {
			return "https://stellafortuna.luminet.cn/app", true
		}
		return "", false
	})
	if err == nil {
		t.Fatal("expected invalid frontend origin")
	}
}

func TestValidateServeRejectsInvalidTrustedProxyCIDR(t *testing.T) {
	cfg, err := LoadFrom(func(name string) (string, bool) {
		values := map[string]string{
			"MYSQL_DSN": "dsn", "REDIS_URL": "redis://localhost", "LINUXDO_CLIENT_ID": "id",
			"LINUXDO_CLIENT_SECRET": "secret", "SESSION_JWT_SECRET": "0123456789abcdef0123456789abcdef",
			"COMPAT_PROXY_SECRET": "abcdef0123456789abcdef0123456789", "TRUSTED_PROXY_CIDRS": "not-a-cidr",
		}
		value, ok := values[name]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateServe(); err == nil {
		t.Fatal("invalid trusted proxy CIDR was accepted")
	}
}

package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestSessionRoundTripAndLegacyIssuerBoundary(t *testing.T) {
	manager, err := NewManager("0123456789abcdef0123456789abcdef", "https://api.luminet.cn/hifumi", "stellafortuna", "https://stellafortuna.luminet.cn")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	manager.now = func() time.Time { return now }
	raw, expires, err := manager.SignSession(Profile{Subject: "42", Username: "hifumi", DisplayName: "Hifumi"})
	if err != nil {
		t.Fatal(err)
	}
	if expires.Sub(now) != SessionLifetime {
		t.Fatalf("unexpected session lifetime: %s", expires.Sub(now))
	}
	claims, err := manager.VerifySession(raw, false)
	if err != nil || claims.Subject != "42" {
		t.Fatalf("verify session: claims=%+v err=%v", claims, err)
	}
	manager.now = func() time.Time { return expires.Add(10 * time.Second) }
	if _, err := manager.VerifySession(raw, false); err == nil {
		t.Fatal("expired session was accepted")
	}
}

func TestLegacyJSGoldenTokenRequiresTrustedCompatibilityPath(t *testing.T) {
	const legacyToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3N0ZWxsYWZvcnR1bmEubHVtaW5ldC5jbiIsImF1ZCI6InN0ZWxsYWZvcnR1bmEiLCJzdWIiOiJsZWdhY3ktdXNlciIsInVzZXJuYW1lIjoibGVnYWN5IiwiaWF0IjoxOTk5OTk5MDAwLCJuYmYiOjE5OTk5OTkwMDAsImV4cCI6MjAwMDAwMDAwMH0.AQI14TFM4ZwPY2c6f74y1qySbyxCoBAjDmwtzyVELHw"
	manager, err := NewManager("0123456789abcdef0123456789abcdef", "https://api.luminet.cn/hifumi", "stellafortuna", "https://stellafortuna.luminet.cn")
	if err != nil {
		t.Fatal(err)
	}
	manager.now = func() time.Time { return time.Unix(1_999_999_500, 0) }
	if _, err := manager.VerifySession(legacyToken, false); err == nil {
		t.Fatal("legacy issuer was accepted without trusted compatibility mode")
	}
	claims, err := manager.VerifySession(legacyToken, true)
	if err != nil || claims.Subject != "legacy-user" {
		t.Fatalf("verify JS golden token: claims=%+v err=%v", claims, err)
	}
}

func TestSessionRejectsTamperingAndNonHS256(t *testing.T) {
	manager, err := NewManager("0123456789abcdef0123456789abcdef", "https://api.luminet.cn/hifumi", "stellafortuna", "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	manager.now = func() time.Time { return now }
	raw, _, err := manager.SignSession(Profile{Subject: "42"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.VerifySession(raw+"tampered", false); err == nil {
		t.Fatal("tampered session was accepted")
	}
	claims := SessionClaims{RegisteredClaims: jwt.RegisteredClaims{
		Issuer: manager.issuer, Subject: "42", Audience: jwt.ClaimStrings{manager.audience},
		ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), NotBefore: jwt.NewNumericDate(now.Add(-time.Second)),
	}}
	hs384, err := jwt.NewWithClaims(jwt.SigningMethodHS384, claims).SignedString(manager.secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.VerifySession(hs384, false); err == nil {
		t.Fatal("non-HS256 session was accepted")
	}
}

func TestOAuthPurposeAndState(t *testing.T) {
	manager, err := NewManager("0123456789abcdef0123456789abcdef", "https://api.luminet.cn/hifumi", "stellafortuna", "")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := manager.SignOAuthState("state-1", "verifier-1", "https://stellafortuna.luminet.cn/", true)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := manager.VerifyOAuthState(raw, "state-1")
	if err != nil || !claims.Compat {
		t.Fatalf("verify oauth state: claims=%+v err=%v", claims, err)
	}
	if _, err := manager.VerifyOAuthState(raw, "different"); err == nil {
		t.Fatal("expected state mismatch")
	}
}

func TestOwnerKeyIsStable(t *testing.T) {
	const expected = "c91b5848841e3c42a5bd8adfa7d365c1d8f96b57e3b9015692c8cee02588fccf"
	if got := OwnerKey("42"); got != expected {
		t.Fatalf("owner key mismatch: %s", got)
	}
}

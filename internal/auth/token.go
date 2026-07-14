package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	SessionLifetime = 31 * 24 * time.Hour
	OAuthLifetime   = 10 * time.Minute
)

var ErrInvalidToken = errors.New("invalid token")

type SessionClaims struct {
	Username  string `json:"username,omitempty"`
	Name      string `json:"name,omitempty"`
	AvatarURL string `json:"avatarUrl,omitempty"`
	Email     string `json:"email,omitempty"`
	jwt.RegisteredClaims
}

type OAuthClaims struct {
	Purpose  string `json:"purpose"`
	State    string `json:"state"`
	Verifier string `json:"verifier"`
	Compat   bool   `json:"compat,omitempty"`
	ReturnTo string `json:"returnTo,omitempty"`
	jwt.RegisteredClaims
}

type Manager struct {
	secret       []byte
	issuer       string
	audience     string
	legacyIssuer string
	now          func() time.Time
}

func NewManager(secret, issuer, audience, legacyIssuer string) (*Manager, error) {
	if len(secret) < 32 {
		return nil, fmt.Errorf("session signing secret must contain at least 32 characters")
	}
	if issuer == "" || audience == "" {
		return nil, fmt.Errorf("session issuer and audience are required")
	}
	return &Manager{
		secret:       []byte(secret),
		issuer:       issuer,
		audience:     audience,
		legacyIssuer: legacyIssuer,
		now:          time.Now,
	}, nil
}

func (m *Manager) SignSession(profile Profile) (string, time.Time, error) {
	now := m.now().UTC()
	expires := now.Add(SessionLifetime)
	claims := SessionClaims{
		Username:  profile.Username,
		Name:      profile.DisplayName,
		AvatarURL: profile.AvatarURL,
		Email:     profile.Email,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			Subject:   profile.Subject,
			Audience:  jwt.ClaimStrings{m.audience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-5 * time.Second)),
			ExpiresAt: jwt.NewNumericDate(expires),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
	return token, expires, err
}

func (m *Manager) VerifySession(raw string, allowLegacyIssuer bool) (*SessionClaims, error) {
	claims := &SessionClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidToken
		}
		return m.secret, nil
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithAudience(m.audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(5*time.Second),
		jwt.WithTimeFunc(m.now),
	)
	if err != nil || !token.Valid || claims.Subject == "" {
		return nil, ErrInvalidToken
	}
	issuerOK := claims.Issuer == m.issuer
	if allowLegacyIssuer && m.legacyIssuer != "" {
		issuerOK = issuerOK || claims.Issuer == m.legacyIssuer
	}
	if !issuerOK {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

func (m *Manager) SignOAuthState(state, verifier, returnTo string, compat bool) (string, error) {
	now := m.now().UTC()
	claims := OAuthClaims{
		Purpose:  "linuxdo-oauth",
		State:    state,
		Verifier: verifier,
		Compat:   compat,
		ReturnTo: returnTo,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			Audience:  jwt.ClaimStrings{m.audience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-5 * time.Second)),
			ExpiresAt: jwt.NewNumericDate(now.Add(OAuthLifetime)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
}

func (m *Manager) VerifyOAuthState(raw, expectedState string) (*OAuthClaims, error) {
	claims := &OAuthClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidToken
		}
		return m.secret, nil
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(m.issuer),
		jwt.WithAudience(m.audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(5*time.Second),
		jwt.WithTimeFunc(m.now),
	)
	if err != nil || !token.Valid || claims.Purpose != "linuxdo-oauth" || claims.State == "" || claims.State != expectedState || claims.Verifier == "" {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

func RandomBase64URL(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func PKCEChallenge(verifier string) string {
	digest := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func OwnerKey(subject string) string {
	digest := sha256.Sum256([]byte("linuxdo:" + subject))
	return hex.EncodeToString(digest[:])
}

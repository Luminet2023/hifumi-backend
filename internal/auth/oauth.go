package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	authorizationEndpoint = "https://connect.linux.do/oauth2/authorize"
	tokenEndpoint         = "https://connect.linux.do/oauth2/token"
	userEndpoint          = "https://connect.linux.do/api/user"
)

type Profile struct {
	Subject     string `json:"subject"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
	AvatarURL   string `json:"avatarUrl"`
	Email       string `json:"email,omitempty"`
}

type Login struct {
	AuthorizationURL string
	StateToken       string
	State            string
}

type Provider struct {
	clientID     string
	clientSecret string
	redirectURI  string
	frontendURL  string
	tokens       *Manager
	httpClient   *http.Client
}

func NewProvider(clientID, clientSecret, redirectURI, frontendURL string, tokens *Manager, httpClient *http.Client) *Provider {
	return &Provider{
		clientID:     clientID,
		clientSecret: clientSecret,
		redirectURI:  redirectURI,
		frontendURL:  frontendURL,
		tokens:       tokens,
		httpClient:   httpClient,
	}
}

func (p *Provider) Begin(compat bool) (Login, error) {
	state, err := RandomBase64URL(32)
	if err != nil {
		return Login{}, err
	}
	verifier, err := RandomBase64URL(48)
	if err != nil {
		return Login{}, err
	}
	stateToken, err := p.tokens.SignOAuthState(state, verifier, p.frontendURL, compat)
	if err != nil {
		return Login{}, err
	}
	authorize, _ := url.Parse(authorizationEndpoint)
	query := authorize.Query()
	query.Set("client_id", p.clientID)
	query.Set("redirect_uri", p.redirectURI)
	query.Set("response_type", "code")
	query.Set("scope", "openid profile email")
	query.Set("state", state)
	query.Set("code_challenge", PKCEChallenge(verifier))
	query.Set("code_challenge_method", "S256")
	authorize.RawQuery = query.Encode()
	return Login{AuthorizationURL: authorize.String(), StateToken: stateToken, State: state}, nil
}

func (p *Provider) Complete(ctx context.Context, code string, state *OAuthClaims) (Profile, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {p.redirectURI},
		"code_verifier": {state.Verifier},
		"client_id":     {p.clientID},
		"client_secret": {p.clientSecret},
	}
	tokenRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Profile{}, err
	}
	tokenRequest.Header.Set("content-type", "application/x-www-form-urlencoded")
	tokenRequest.Header.Set("accept", "application/json")
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := p.fetchJSON(tokenRequest, &token); err != nil {
		return Profile{}, err
	}
	if token.AccessToken == "" {
		return Profile{}, fmt.Errorf("linux do token response did not contain access_token")
	}
	profileRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, userEndpoint, nil)
	if err != nil {
		return Profile{}, err
	}
	profileRequest.Header.Set("authorization", "Bearer "+token.AccessToken)
	profileRequest.Header.Set("accept", "application/json")
	var payload map[string]any
	if err := p.fetchJSON(profileRequest, &payload); err != nil {
		return Profile{}, err
	}
	profile := normalizeProfile(payload)
	if profile.Subject == "" {
		return Profile{}, fmt.Errorf("linux do profile did not contain a subject")
	}
	return profile, nil
}

func (p *Provider) fetchJSON(request *http.Request, target any) error {
	response, err := p.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("linux do oauth upstream returned %d", response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target); err != nil {
		return fmt.Errorf("decode linux do response: %w", err)
	}
	return nil
}

func normalizeProfile(payload map[string]any) Profile {
	first := func(keys ...string) string {
		for _, key := range keys {
			if value, ok := payload[key]; ok {
				if text := strings.TrimSpace(fmt.Sprint(value)); text != "" && text != "<nil>" {
					return text
				}
			}
		}
		return ""
	}
	username := first("username", "login")
	name := first("displayName", "name", "username", "login")
	if name == "" {
		name = "Linux DO 用户"
	}
	return Profile{
		Subject:     first("subject", "sub", "id"),
		Username:    username,
		DisplayName: name,
		AvatarURL:   first("avatarUrl", "avatar_url"),
		Email:       first("email"),
	}
}

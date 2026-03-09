package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/alexedwards/scs/v2"
	"github.com/steipete/discrawl/internal/crypto"
	"github.com/steipete/discrawl/internal/store"
	"golang.org/x/oauth2"
)

const (
	discordAuthURL  = "https://discord.com/api/oauth2/authorize"
	discordTokenURL = "https://discord.com/api/oauth2/token"
	discordAPIBase  = "https://discord.com/api/v10"

	sessionKeyState  = "oauth_state"
	sessionKeyUserID = "user_id"
)

// OAuthConfig holds Discord OAuth2 settings.
type OAuthConfig struct {
	ClientID     string
	ClientSecret string
	RedirectURI  string
}

// NewOAuth2Config creates an oauth2.Config for Discord.
func NewOAuth2Config(cfg OAuthConfig) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  cfg.RedirectURI,
		Scopes:       []string{"identify", "guilds"},
		Endpoint: oauth2.Endpoint{
			AuthURL:  discordAuthURL,
			TokenURL: discordTokenURL,
		},
	}
}

// HandleLogin redirects to Discord OAuth2.
func HandleLogin(sm *scs.SessionManager, oauthCfg *oauth2.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, err := generateState()
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		sm.Put(r.Context(), sessionKeyState, state)
		url := oauthCfg.AuthCodeURL(state, oauth2.AccessTypeOnline)
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	}
}

// HandleCallback exchanges code for token, fetches user+guilds, creates session.
func HandleCallback(sm *scs.SessionManager, oauthCfg *oauth2.Config, meta *store.MetaStore, encryptionKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		storedState := sm.GetString(r.Context(), sessionKeyState)
		if storedState == "" || storedState != r.URL.Query().Get("state") {
			http.Error(w, "invalid state", http.StatusBadRequest)
			return
		}
		sm.Remove(r.Context(), sessionKeyState)

		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "missing code", http.StatusBadRequest)
			return
		}

		token, err := oauthCfg.Exchange(r.Context(), code)
		if err != nil {
			http.Error(w, "token exchange failed", http.StatusInternalServerError)
			return
		}

		client := oauthCfg.Client(r.Context(), token)

		user, err := fetchDiscordUser(r.Context(), client)
		if err != nil {
			http.Error(w, "failed to fetch user", http.StatusInternalServerError)
			return
		}

		guilds, err := fetchDiscordGuilds(r.Context(), client)
		if err != nil {
			// Non-fatal: proceed without guilds
			guilds = nil
		}

		// Encrypt tokens before storing.
		encAccessToken, err := crypto.Encrypt(token.AccessToken, encryptionKey)
		if err != nil {
			http.Error(w, "failed to encrypt access token", http.StatusInternalServerError)
			return
		}
		encRefreshToken, err := crypto.Encrypt(token.RefreshToken, encryptionKey)
		if err != nil {
			http.Error(w, "failed to encrypt refresh token", http.StatusInternalServerError)
			return
		}

		now := time.Now().UTC().Format(time.RFC3339Nano)
		if err := meta.UpsertUser(r.Context(), store.UserRecord{
			ID:           user.ID,
			Username:     user.Username,
			Avatar:       user.Avatar,
			AccessToken:  encAccessToken,
			RefreshToken: encRefreshToken,
			TokenExpiry:  token.Expiry.UTC().Format(time.RFC3339Nano),
			CreatedAt:    now,
			UpdatedAt:    now,
		}); err != nil {
			http.Error(w, "failed to upsert user", http.StatusInternalServerError)
			return
		}

		for _, g := range guilds {
			_ = meta.UpsertUserGuild(r.Context(), store.UserGuildRecord{
				UserID:    user.ID,
				GuildID:   g.ID,
				GuildName: g.Name,
			})
		}

		sm.Put(r.Context(), sessionKeyUserID, user.ID)
		http.Redirect(w, r, "/app/guilds", http.StatusSeeOther)
	}
}

// HandleLogout destroys the session.
func HandleLogout(sm *scs.SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := sm.Destroy(r.Context()); err != nil {
			http.Error(w, "logout failed", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

func generateState() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

type discordUser struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Avatar   string `json:"avatar"`
}

type discordGuild struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func fetchDiscordUser(ctx context.Context, client *http.Client) (*discordUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discordAPIBase+"/users/@me", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discord /users/@me returned %d", resp.StatusCode)
	}
	var u discordUser
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		return nil, err
	}
	return &u, nil
}

func fetchDiscordGuilds(ctx context.Context, client *http.Client) ([]discordGuild, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discordAPIBase+"/users/@me/guilds", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discord /users/@me/guilds returned %d", resp.StatusCode)
	}
	var guilds []discordGuild
	if err := json.NewDecoder(resp.Body).Decode(&guilds); err != nil {
		return nil, err
	}
	return guilds, nil
}

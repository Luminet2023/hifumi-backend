package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Luminet2023/hifumi-backend/internal/auth"
)

const maxProfileFutureSkew = 5 * time.Minute

func (s *Store) UpsertProfile(ctx context.Context, ownerKey string, profile auth.Profile, lastLoginAtMs uint64) (auth.Profile, error) {
	profile = normalizedProfile(profile)
	if ownerKey == "" || profile.Subject == "" {
		return auth.Profile{}, fmt.Errorf("owner key and Linux DO subject are required")
	}
	now := uint64(time.Now().UnixMilli())
	if lastLoginAtMs == 0 {
		lastLoginAtMs = now
	}
	maximum := now + uint64(maxProfileFutureSkew/time.Millisecond)
	if lastLoginAtMs > maximum {
		lastLoginAtMs = maximum
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return auth.Profile{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		"INSERT IGNORE INTO sync_owners (owner_key, created_at_ms) VALUES (?, ?)", ownerKey, now,
	); err != nil {
		return auth.Profile{}, fmt.Errorf("ensure profile owner: %w", err)
	}
	var email any
	if profile.Email != "" {
		email = profile.Email
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO user_profiles
		 (owner_key, linuxdo_subject, username, display_name, avatar_url, email,
		  created_at_ms, updated_at_ms, last_login_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		 linuxdo_subject = VALUES(linuxdo_subject), username = VALUES(username),
		 display_name = VALUES(display_name), avatar_url = VALUES(avatar_url),
		 email = COALESCE(VALUES(email), email), updated_at_ms = VALUES(updated_at_ms),
		 last_login_at_ms = GREATEST(last_login_at_ms, VALUES(last_login_at_ms))`,
		ownerKey, profile.Subject, profile.Username, profile.DisplayName, profile.AvatarURL,
		email, now, now, lastLoginAtMs,
	)
	if err != nil {
		return auth.Profile{}, fmt.Errorf("upsert user profile: %w", err)
	}
	var stored auth.Profile
	var storedEmail sql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT linuxdo_subject, username, display_name, avatar_url, email
		 FROM user_profiles WHERE owner_key = ?`, ownerKey,
	).Scan(&stored.Subject, &stored.Username, &stored.DisplayName, &stored.AvatarURL, &storedEmail)
	if err != nil {
		return auth.Profile{}, fmt.Errorf("read user profile: %w", err)
	}
	if storedEmail.Valid {
		stored.Email = storedEmail.String
	}
	if err := tx.Commit(); err != nil {
		return auth.Profile{}, err
	}
	return stored, nil
}

func normalizedProfile(profile auth.Profile) auth.Profile {
	profile.Subject = truncate(strings.TrimSpace(profile.Subject), 128)
	profile.Username = truncate(strings.TrimSpace(profile.Username), 128)
	profile.DisplayName = truncate(strings.TrimSpace(profile.DisplayName), 256)
	if profile.DisplayName == "" {
		profile.DisplayName = profile.Username
	}
	if profile.DisplayName == "" {
		profile.DisplayName = "Linux DO 用户"
	}
	profile.AvatarURL = truncate(strings.TrimSpace(profile.AvatarURL), 2048)
	profile.Email = truncate(strings.TrimSpace(profile.Email), 320)
	return profile
}

func truncate(value string, maximum int) string {
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum])
}

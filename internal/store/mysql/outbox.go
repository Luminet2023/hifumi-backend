package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Luminet2023/hifumi-backend/internal/realtime"
)

type HintPublisher interface {
	PublishHint(context.Context, realtime.Hint) error
}

func RunOutbox(ctx context.Context, db *sql.DB, publisher HintPublisher, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		published, err := publishOne(ctx, db, publisher)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.WarnContext(ctx, "realtime outbox publish failed", "error", err)
		}
		if published {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func publishOne(ctx context.Context, db *sql.DB, publisher HintPublisher) (bool, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var id uint64
	var hint realtime.Hint
	err = tx.QueryRowContext(ctx,
		`SELECT id, owner_key, baseline_id, server_cursor, server_version,
		        COALESCE(origin_connection_id, '')
		 FROM realtime_outbox
		 WHERE published_at_ms IS NULL
		 ORDER BY id ASC LIMIT 1 FOR UPDATE SKIP LOCKED`,
	).Scan(&id, &hint.OwnerKey, &hint.BaselineID, &hint.ServerCursor, &hint.ServerVersion, &hint.OriginConnectionID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	publishContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = publisher.PublishHint(publishContext, hint)
	cancel()
	if err != nil {
		message := err.Error()
		if len(message) > 1024 {
			message = message[:1024]
		}
		if _, updateErr := tx.ExecContext(ctx,
			`UPDATE realtime_outbox
			 SET publish_attempts = publish_attempts + 1, last_error = ? WHERE id = ?`, message, id,
		); updateErr != nil {
			return false, updateErr
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return false, commitErr
		}
		return false, fmt.Errorf("publish outbox event %d: %w", id, err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE realtime_outbox
		 SET published_at_ms = ?, publish_attempts = publish_attempts + 1, last_error = NULL
		 WHERE id = ?`, uint64(time.Now().UnixMilli()), id,
	); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

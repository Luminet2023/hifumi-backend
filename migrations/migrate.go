package migrations

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
)

const (
	CurrentVersion = uint64(1)
	advisoryLock   = "study-list:schema-migrations"
)

//go:embed 001_sync_core.up.sql
var migrationOne string

func Up(ctx context.Context, db *sql.DB) error {
	connection, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	var acquired int
	if err := connection.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", advisoryLock).Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	if acquired != 1 {
		return fmt.Errorf("migration advisory lock timed out")
	}
	defer connection.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", advisoryLock)

	statements := splitStatements(migrationOne)
	if len(statements) == 0 {
		return fmt.Errorf("embedded migration is empty")
	}
	if _, err := connection.ExecContext(ctx, statements[0]); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	var dirty bool
	err = connection.QueryRowContext(ctx, "SELECT dirty FROM schema_migrations WHERE version = ?", CurrentVersion).Scan(&dirty)
	if err == nil && !dirty {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read schema migration: %w", err)
	}
	if _, err := connection.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, dirty) VALUES (?, TRUE)
		 ON DUPLICATE KEY UPDATE dirty = TRUE`, CurrentVersion,
	); err != nil {
		return fmt.Errorf("mark schema migration dirty: %w", err)
	}
	for index, statement := range statements[1:] {
		if _, err := connection.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migration %d statement %d: %w", CurrentVersion, index+2, err)
		}
	}
	if _, err := connection.ExecContext(ctx,
		"UPDATE schema_migrations SET dirty = FALSE, applied_at = CURRENT_TIMESTAMP(6) WHERE version = ?", CurrentVersion,
	); err != nil {
		return fmt.Errorf("mark schema migration clean: %w", err)
	}
	return nil
}

func Check(ctx context.Context, db *sql.DB) error {
	var version uint64
	var dirty bool
	if err := db.QueryRowContext(ctx,
		"SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1",
	).Scan(&version, &dirty); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version != CurrentVersion || dirty {
		return fmt.Errorf("schema version is %d dirty=%t, expected %d clean", version, dirty, CurrentVersion)
	}
	return nil
}

func splitStatements(source string) []string {
	parts := strings.Split(source, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		if statement := strings.TrimSpace(part); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

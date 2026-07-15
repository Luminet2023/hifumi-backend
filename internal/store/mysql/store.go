package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	synccore "github.com/Luminet2023/hifumi-backend/internal/sync"
)

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// Open 创建并验证 MySQL 连接。连接池大小和生命周期应由调用方按部署规格设置。
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (s *Store) WithOwnerTransaction(
	ctx context.Context,
	ownerKey string,
	fn func(synccore.Tx) error,
) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := uint64(time.Now().UnixMilli())
	if _, err := tx.ExecContext(ctx,
		"INSERT IGNORE INTO sync_owners (owner_key, created_at_ms) VALUES (?, ?)",
		ownerKey,
		now,
	); err != nil {
		return fmt.Errorf("ensure sync owner: %w", err)
	}
	var lockedOwner string
	if err := tx.QueryRowContext(ctx,
		"SELECT owner_key FROM sync_owners WHERE owner_key = ? FOR UPDATE",
		ownerKey,
	).Scan(&lockedOwner); err != nil {
		return fmt.Errorf("lock sync owner: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT IGNORE INTO sync_heads (owner_key, `cursor`) VALUES (?, 0)",
		ownerKey,
	); err != nil {
		return fmt.Errorf("ensure sync head: %w", err)
	}

	ownerTx := &ownerTx{tx: tx, ownerKey: ownerKey}
	if err := fn(ownerTx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *Store) ReadFeedSnapshot(
	ctx context.Context,
	ownerKey string,
	afterCursor uint64,
	limit int,
) (synccore.FeedSnapshot, error) {
	// Feed 读取只需要一个很短的 REPEATABLE READ 一致性快照。这里不创建
	// owner/head，也不使用 FOR UPDATE，因此 SSE 长连接不会持有事务或 owner 锁。
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return synccore.FeedSnapshot{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	snapshot := synccore.FeedSnapshot{}
	err = tx.QueryRowContext(ctx,
		`SELECT `+"`cursor`"+`, baseline_id, version, updated_at_ms, progress_day
		 FROM sync_heads
		 WHERE owner_key = ? AND baseline_id IS NOT NULL`,
		ownerKey,
	).Scan(
		&snapshot.HeadCursor,
		&snapshot.Lineage.BaselineID,
		&snapshot.Lineage.Version,
		&snapshot.Lineage.UpdatedAtMs,
		&snapshot.Lineage.ProgressDay,
	)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return synccore.FeedSnapshot{}, err
		}
		committed = true
		return synccore.FeedSnapshot{}, synccore.ErrNotFound
	}
	if err != nil {
		return synccore.FeedSnapshot{}, err
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT `+"`cursor`"+`, entity_key, value_json, deleted, device_id, client_time_ms, op_id
		 FROM sync_operations
		 WHERE owner_key = ? AND `+"`cursor`"+` > ?
		 ORDER BY `+"`cursor`"+` ASC LIMIT ?`,
		ownerKey,
		afterCursor,
		limit,
	)
	if err != nil {
		return synccore.FeedSnapshot{}, err
	}
	snapshot.Changes, err = scanChanges(rows)
	closeErr := rows.Close()
	if err != nil {
		return synccore.FeedSnapshot{}, err
	}
	if closeErr != nil {
		return synccore.FeedSnapshot{}, closeErr
	}
	if err := tx.Commit(); err != nil {
		return synccore.FeedSnapshot{}, err
	}
	committed = true
	return snapshot, nil
}

type ownerTx struct {
	tx       *sql.Tx
	ownerKey string
}

func (t *ownerTx) CurrentCursor(ctx context.Context) (uint64, error) {
	var cursor uint64
	err := t.tx.QueryRowContext(ctx,
		"SELECT `cursor` FROM sync_heads WHERE owner_key = ?",
		t.ownerKey,
	).Scan(&cursor)
	return cursor, err
}

func (t *ownerTx) NextCursor(ctx context.Context) (uint64, error) {
	if _, err := t.tx.ExecContext(ctx,
		"UPDATE sync_heads SET `cursor` = `cursor` + 1 WHERE owner_key = ?",
		t.ownerKey,
	); err != nil {
		return 0, err
	}
	return t.CurrentCursor(ctx)
}

func (t *ownerTx) GetLineage(ctx context.Context) (synccore.Lineage, error) {
	var lineage synccore.Lineage
	err := t.tx.QueryRowContext(ctx,
		`SELECT baseline_id, version, updated_at_ms, progress_day
		 FROM sync_heads
		 WHERE owner_key = ? AND baseline_id IS NOT NULL`,
		t.ownerKey,
	).Scan(&lineage.BaselineID, &lineage.Version, &lineage.UpdatedAtMs, &lineage.ProgressDay)
	if errors.Is(err, sql.ErrNoRows) {
		return synccore.Lineage{}, synccore.ErrNotFound
	}
	return lineage, err
}

func (t *ownerTx) InsertLineage(ctx context.Context, lineage synccore.Lineage) error {
	return t.writeLineage(ctx, lineage)
}

func (t *ownerTx) SetLineage(ctx context.Context, lineage synccore.Lineage) error {
	return t.writeLineage(ctx, lineage)
}

func (t *ownerTx) writeLineage(ctx context.Context, lineage synccore.Lineage) error {
	_, err := t.tx.ExecContext(ctx,
		`UPDATE sync_heads
		 SET baseline_id = ?, version = ?, updated_at_ms = ?, progress_day = ?
		 WHERE owner_key = ?`,
		lineage.BaselineID,
		lineage.Version,
		lineage.UpdatedAtMs,
		lineage.ProgressDay,
		t.ownerKey,
	)
	if err != nil {
		return err
	}
	return nil
}

func (t *ownerTx) IncrementLineage(ctx context.Context, updatedAtMs uint64, progressDay string) (synccore.Lineage, error) {
	if _, err := t.tx.ExecContext(ctx,
		`UPDATE sync_heads
		 SET version = version + 1, updated_at_ms = ?, progress_day = ?
		 WHERE owner_key = ? AND baseline_id IS NOT NULL`,
		updatedAtMs,
		progressDay,
		t.ownerKey,
	); err != nil {
		return synccore.Lineage{}, err
	}
	return t.GetLineage(ctx)
}

func (t *ownerTx) GetRecord(ctx context.Context, entityKey string) (*syncv1.Change, error) {
	change, err := scanChange(t.tx.QueryRowContext(ctx,
		`SELECT `+"`cursor`"+`, entity_key, value_json, deleted, device_id, client_time_ms, op_id
		 FROM sync_records WHERE owner_key = ? AND entity_key = ?`,
		t.ownerKey,
		entityKey,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, synccore.ErrNotFound
	}
	return change, err
}

func (t *ownerTx) ListRecords(ctx context.Context) ([]*syncv1.Change, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+"`cursor`"+`, entity_key, value_json, deleted, device_id, client_time_ms, op_id
		 FROM sync_records WHERE owner_key = ? ORDER BY entity_key ASC`,
		t.ownerKey,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChanges(rows)
}

func (t *ownerTx) GetOperation(ctx context.Context, cursor uint64) (*syncv1.Change, error) {
	change, err := scanChange(t.tx.QueryRowContext(ctx,
		`SELECT `+"`cursor`"+`, entity_key, value_json, deleted, device_id, client_time_ms, op_id
		 FROM sync_operations
		 WHERE owner_key = ? AND `+"`cursor`"+` = ?`,
		t.ownerKey,
		cursor,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, synccore.ErrNotFound
	}
	return change, err
}

func (t *ownerTx) PullOperations(ctx context.Context, afterCursor uint64, limit int) ([]*syncv1.Change, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+"`cursor`"+`, entity_key, value_json, deleted, device_id, client_time_ms, op_id
		 FROM sync_operations
		 WHERE owner_key = ? AND `+"`cursor`"+` > ?
		 ORDER BY `+"`cursor`"+` ASC LIMIT ?`,
		t.ownerKey,
		afterCursor,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChanges(rows)
}

func (t *ownerTx) AppendOperation(ctx context.Context, change *syncv1.Change) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO sync_operations
		 (owner_key, `+"`cursor`"+`, op_id, entity_key, value_json, deleted, device_id, client_time_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ownerKey,
		change.GetCursor(),
		change.GetOpId(),
		change.GetEntityKey(),
		nonNullBlob(change.GetValueJson()),
		change.GetDeleted(),
		change.GetDeviceId(),
		change.GetClientTimeMs(),
	)
	return err
}

func (t *ownerTx) UpsertRecord(ctx context.Context, change *syncv1.Change) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO sync_records
		 (owner_key, entity_key, `+"`cursor`"+`, value_json, deleted, device_id, client_time_ms, op_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		 `+"`cursor`"+` = VALUES(`+"`cursor`"+`), value_json = VALUES(value_json), deleted = VALUES(deleted),
		 device_id = VALUES(device_id), client_time_ms = VALUES(client_time_ms), op_id = VALUES(op_id)`,
		t.ownerKey,
		change.GetEntityKey(),
		change.GetCursor(),
		nonNullBlob(change.GetValueJson()),
		change.GetDeleted(),
		change.GetDeviceId(),
		change.GetClientTimeMs(),
		change.GetOpId(),
	)
	return err
}

func (t *ownerTx) GetReceipt(ctx context.Context, opID string) (synccore.Receipt, error) {
	var receipt synccore.Receipt
	err := t.tx.QueryRowContext(ctx,
		`SELECT op_id, server_cursor, conflict, applied
		 FROM sync_receipts WHERE owner_key = ? AND op_id = ?`,
		t.ownerKey,
		opID,
	).Scan(&receipt.OpID, &receipt.ServerCursor, &receipt.Conflict, &receipt.Applied)
	if errors.Is(err, sql.ErrNoRows) {
		return synccore.Receipt{}, synccore.ErrNotFound
	}
	return receipt, err
}

func (t *ownerTx) InsertReceipt(ctx context.Context, receipt synccore.Receipt) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO sync_receipts (owner_key, op_id, server_cursor, conflict, applied)
		 VALUES (?, ?, ?, ?, ?)`,
		t.ownerKey,
		receipt.OpID,
		receipt.ServerCursor,
		receipt.Conflict,
		receipt.Applied,
	)
	return err
}

func (t *ownerTx) GetResolutionReceipt(ctx context.Context, requestID string) (synccore.ResolutionReceipt, error) {
	var receipt synccore.ResolutionReceipt
	var choice uint32
	err := t.tx.QueryRowContext(ctx,
		`SELECT request_id, local_baseline_id, expected_server_baseline_id,
		 expected_server_version, choice, result_baseline_id, result_cursor,
		 result_version, result_updated_at_ms
		 FROM baseline_resolutions WHERE owner_key = ? AND request_id = ?`,
		t.ownerKey,
		requestID,
	).Scan(
		&receipt.RequestID,
		&receipt.LocalBaselineID,
		&receipt.ExpectedServerBaselineID,
		&receipt.ExpectedServerVersion,
		&choice,
		&receipt.ResultBaselineID,
		&receipt.ResultCursor,
		&receipt.ResultVersion,
		&receipt.ResultUpdatedAtMs,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return synccore.ResolutionReceipt{}, synccore.ErrNotFound
	}
	receipt.Choice = syncv1.BaselineChoice(choice)
	return receipt, err
}

func (t *ownerTx) InsertResolutionReceipt(ctx context.Context, receipt synccore.ResolutionReceipt) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO baseline_resolutions
		 (owner_key, request_id, local_baseline_id, expected_server_baseline_id,
		  expected_server_version, choice, result_baseline_id, result_cursor,
		  result_version, result_updated_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ownerKey,
		receipt.RequestID,
		receipt.LocalBaselineID,
		receipt.ExpectedServerBaselineID,
		receipt.ExpectedServerVersion,
		uint32(receipt.Choice),
		receipt.ResultBaselineID,
		receipt.ResultCursor,
		receipt.ResultVersion,
		receipt.ResultUpdatedAtMs,
	)
	return err
}

func (t *ownerTx) ClearCurrentBaseline(ctx context.Context) error {
	statements := []string{
		"DELETE FROM sync_receipts WHERE owner_key = ?",
		"DELETE FROM sync_operations WHERE owner_key = ?",
		"DELETE FROM sync_records WHERE owner_key = ?",
		"DELETE FROM baseline_resolutions WHERE owner_key = ?",
		"UPDATE sync_heads SET `cursor` = 0 WHERE owner_key = ?",
	}
	for _, statement := range statements {
		if _, err := t.tx.ExecContext(ctx, statement, t.ownerKey); err != nil {
			return err
		}
	}
	return nil
}

func (t *ownerTx) GetArchiveHead(ctx context.Context, baselineID string) (synccore.ArchiveHead, error) {
	var head synccore.ArchiveHead
	err := t.tx.QueryRowContext(ctx,
		`SELECT `+"`cursor`"+`, server_version, updated_at_ms
		 FROM sync_archive_heads
		 WHERE owner_key = ? AND baseline_id = ?`,
		t.ownerKey,
		baselineID,
	).Scan(&head.Cursor, &head.Version, &head.UpdatedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return synccore.ArchiveHead{}, synccore.ErrNotFound
	}
	return head, err
}

func (t *ownerTx) ArchiveChange(
	ctx context.Context,
	baselineID string,
	serverVersion uint64,
	archivedAtMs uint64,
	change *syncv1.Change,
) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO sync_archive_changes
		 (owner_key, baseline_id, `+"`cursor`"+`, op_id, entity_key, value_json, deleted,
		  device_id, client_time_ms, server_version, archived_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ownerKey,
		baselineID,
		change.GetCursor(),
		change.GetOpId(),
		change.GetEntityKey(),
		nonNullBlob(change.GetValueJson()),
		change.GetDeleted(),
		change.GetDeviceId(),
		change.GetClientTimeMs(),
		serverVersion,
		archivedAtMs,
	)
	return err
}

func nonNullBlob(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}

func (t *ownerTx) UpsertArchiveHead(
	ctx context.Context,
	baselineID string,
	cursor uint64,
	serverVersion uint64,
	updatedAtMs uint64,
) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO sync_archive_heads
		 (owner_key, baseline_id, `+"`cursor`"+`, server_version, updated_at_ms)
		 VALUES (?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE `+"`cursor`"+` = VALUES(`+"`cursor`"+`), server_version = VALUES(server_version),
		 updated_at_ms = VALUES(updated_at_ms)`,
		t.ownerKey,
		baselineID,
		cursor,
		serverVersion,
		updatedAtMs,
	)
	return err
}

func (t *ownerTx) InsertRealtimeEvent(ctx context.Context, event synccore.RealtimeEvent) error {
	var originConnectionID any
	if event.OriginConnectionID != "" {
		originConnectionID = event.OriginConnectionID
	}
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO realtime_outbox
		 (owner_key, event_type, baseline_id, server_cursor, server_version,
		  origin_connection_id, created_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		t.ownerKey,
		event.Type,
		event.BaselineID,
		event.ServerCursor,
		event.ServerVersion,
		originConnectionID,
		event.CreatedAtMs,
	)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanChange(row scanner) (*syncv1.Change, error) {
	change := &syncv1.Change{}
	err := row.Scan(
		&change.Cursor,
		&change.EntityKey,
		&change.ValueJson,
		&change.Deleted,
		&change.DeviceId,
		&change.ClientTimeMs,
		&change.OpId,
	)
	return change, err
}

func scanChanges(rows *sql.Rows) ([]*syncv1.Change, error) {
	changes := make([]*syncv1.Change, 0)
	for rows.Next() {
		change, err := scanChange(rows)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return changes, nil
}

var _ synccore.Store = (*Store)(nil)
var _ synccore.Tx = (*ownerTx)(nil)

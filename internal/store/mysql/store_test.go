package mysql

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	synccore "github.com/Luminet2023/hifumi-backend/internal/sync"
)

const ownerKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestNonNullBlobNormalizesNil(t *testing.T) {
	if value := nonNullBlob(nil); value == nil || len(value) != 0 {
		t.Fatalf("nil value was not normalized to an empty blob: %#v", value)
	}
	original := []byte(`{"done":true}`)
	if value := nonNullBlob(original); string(value) != string(original) {
		t.Fatalf("non-empty value changed: got %q want %q", value, original)
	}
}

func TestWithOwnerTransactionLocksOwnerForUpdate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT IGNORE INTO sync_owners (owner_key, created_at_ms) VALUES (?, ?)",
	)).WithArgs(ownerKey, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT owner_key FROM sync_owners WHERE owner_key = ? FOR UPDATE",
	)).WithArgs(ownerKey).WillReturnRows(sqlmock.NewRows([]string{"owner_key"}).AddRow(ownerKey))
	mock.ExpectExec(regexp.QuoteMeta(
		"INSERT IGNORE INTO sync_heads (owner_key, `cursor`) VALUES (?, 0)",
	)).WithArgs(ownerKey).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT `cursor` FROM sync_heads WHERE owner_key = ?",
	)).WithArgs(ownerKey).WillReturnRows(sqlmock.NewRows([]string{"cursor"}).AddRow(uint64(0)))
	mock.ExpectCommit()

	store := New(db)
	err = store.WithOwnerTransaction(context.Background(), ownerKey, func(tx synccore.Tx) error {
		cursor, err := tx.CurrentCursor(context.Background())
		if err != nil {
			return err
		}
		if cursor != 0 {
			t.Fatalf("cursor = %d, want 0", cursor)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWithOwnerTransactionRollsBackCallbackFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO sync_owners").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT owner_key FROM sync_owners .* FOR UPDATE").WillReturnRows(
		sqlmock.NewRows([]string{"owner_key"}).AddRow(ownerKey),
	)
	mock.ExpectExec("INSERT IGNORE INTO sync_heads").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	want := errors.New("callback failed")
	err = New(db).WithOwnerTransaction(context.Background(), ownerKey, func(_ synccore.Tx) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReadFeedSnapshotUsesShortConsistentReadTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT `cursor`, baseline_id, version, updated_at_ms, progress_day FROM sync_heads WHERE owner_key = ? AND baseline_id IS NOT NULL",
	)).WithArgs(ownerKey).WillReturnRows(sqlmock.NewRows([]string{
		"cursor", "baseline_id", "version", "updated_at_ms", "progress_day",
	}).AddRow(uint64(3), "baseline_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", uint64(2), uint64(1234), "2026-07-15"))
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT `cursor`, entity_key, value_json, deleted, device_id, client_time_ms, op_id FROM sync_operations WHERE owner_key = ? AND `cursor` > ? ORDER BY `cursor` ASC LIMIT ?",
	)).WithArgs(ownerKey, uint64(1), 2).WillReturnRows(sqlmock.NewRows([]string{
		"cursor", "entity_key", "value_json", "deleted", "device_id", "client_time_ms", "op_id",
	}).AddRow(uint64(2), "stella/v1/preference/theme", []byte(`"dark"`), false, "device_alpha", uint64(100), "operation_feed_0002").
		AddRow(uint64(3), "stella/v1/preference/fontFamily", []byte(`"system"`), false, "device_beta", uint64(101), "operation_feed_0003"))
	mock.ExpectCommit()

	snapshot, err := New(db).ReadFeedSnapshot(context.Background(), ownerKey, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.HeadCursor != 3 || snapshot.Lineage.Version != 2 || snapshot.Lineage.ProgressDay != "2026-07-15" ||
		len(snapshot.Changes) != 2 || snapshot.Changes[0].GetCursor() != 2 || snapshot.Changes[1].GetCursor() != 3 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReadFeedSnapshotWithoutHeadReturnsNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM sync_heads").WithArgs(ownerKey).WillReturnRows(sqlmock.NewRows([]string{
		"cursor", "baseline_id", "version", "updated_at_ms", "progress_day",
	}))
	mock.ExpectCommit()

	_, err = New(db).ReadFeedSnapshot(context.Background(), ownerKey, 0, 1)
	if !errors.Is(err, synccore.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

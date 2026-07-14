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

package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
)

type publisherFunc func(context.Context, realtime.Hint) error

func (function publisherFunc) PublishHint(ctx context.Context, hint realtime.Hint) error {
	return function(ctx, hint)
}

func TestPublishOneRecordsFailureForRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, owner_key, baseline_id").WillReturnRows(sqlmock.NewRows([]string{
		"id", "owner_key", "baseline_id", "server_cursor", "server_version", "origin_connection_id",
	}).AddRow(uint64(7), "owner", "baseline_0123456789abcdef0123456789abcdef", uint64(3), uint64(2), "writer"))
	mock.ExpectExec("UPDATE realtime_outbox").WithArgs("temporary Redis failure", uint64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	published, err := publishOne(context.Background(), db, publisherFunc(func(context.Context, realtime.Hint) error {
		return errors.New("temporary Redis failure")
	}))
	if published || err == nil {
		t.Fatalf("unexpected result: published=%t err=%v", published, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPublishOneMarksSuccessfulDelivery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, owner_key, baseline_id").WillReturnRows(sqlmock.NewRows([]string{
		"id", "owner_key", "baseline_id", "server_cursor", "server_version", "origin_connection_id",
	}).AddRow(uint64(8), "owner", "baseline_0123456789abcdef0123456789abcdef", uint64(4), uint64(3), ""))
	mock.ExpectExec("UPDATE realtime_outbox").WithArgs(sqlmock.AnyArg(), uint64(8)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	published, err := publishOne(context.Background(), db, publisherFunc(func(_ context.Context, hint realtime.Hint) error {
		if hint.OwnerKey != "owner" || hint.ServerCursor != 4 {
			t.Fatalf("unexpected hint: %+v", hint)
		}
		return nil
	}))
	if err != nil || !published {
		t.Fatalf("unexpected result: published=%t err=%v", published, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"github.com/Luminet2023/hifumi-backend/internal/auth"
	"github.com/Luminet2023/hifumi-backend/internal/realtime"
	mysqlstore "github.com/Luminet2023/hifumi-backend/internal/store/mysql"
	syncservice "github.com/Luminet2023/hifumi-backend/internal/sync"
	"github.com/Luminet2023/hifumi-backend/migrations"
	drivermysql "github.com/go-sql-driver/mysql"
)

const (
	baselineA = "baseline_0123456789abcdef0123456789abcdef"
	baselineB = "baseline_abcdef0123456789abcdef0123456789"
	baselineC = "baseline_cccccccccccccccccccccccccccccccc"
)

type flakyPublisher struct {
	delegate *realtime.Client
	mu       sync.Mutex
	calls    int
}

func (p *flakyPublisher) PublishHint(ctx context.Context, hint realtime.Hint) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call == 1 {
		return errors.New("temporary Redis failure")
	}
	return p.delegate.PublishHint(ctx, hint)
}

func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var admin *sql.DB
	var databaseName string
	if adminDSN := os.Getenv("MYSQL_TEST_ADMIN_DSN"); adminDSN != "" {
		var err error
		admin, err = mysqlstore.Open(ctx, adminDSN)
		if err != nil {
			t.Fatal(err)
		}
		databaseName = fmt.Sprintf("study_list_test_%d", time.Now().UnixNano())
		if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+databaseName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
			admin.Close()
			t.Fatal(err)
		}
		parsed, err := drivermysql.ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		parsed.DBName = databaseName
		dsn = parsed.FormatDSN()
	}
	db, err := mysqlstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		if admin != nil {
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = admin.ExecContext(cleanupContext, "DROP DATABASE IF EXISTS `"+databaseName+"`")
			admin.Close()
		}
	})
	if err := migrations.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Check(ctx, db); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestMySQLConcurrentDiffFeedReplayConflictAndBaselineArchive(t *testing.T) {
	db := integrationDB(t)
	store := mysqlstore.New(db)
	service := syncservice.NewService(store, syncservice.WithBaselineIDGenerator(func() (string, error) {
		return baselineC, nil
	}))
	subject := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	owner := auth.OwnerKey(subject)
	metadata := syncservice.CommandMetadata{OwnerKey: owner}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	profile, err := store.UpsertProfile(ctx, owner, auth.Profile{
		Subject: subject, Username: "tester", DisplayName: "Tester", Email: "first@example.test",
	}, uint64(time.Now().UnixMilli()))
	if err != nil || profile.Email != "first@example.test" {
		t.Fatalf("initial profile upsert: profile=%+v err=%v", profile, err)
	}
	profile, err = store.UpsertProfile(ctx, owner, auth.Profile{
		Subject: subject, Username: "tester", DisplayName: "Tester Updated",
	}, uint64(time.Now().UnixMilli()))
	if err != nil || profile.Email != "first@example.test" {
		t.Fatalf("empty email overwrote stored email: profile=%+v err=%v", profile, err)
	}

	seed, err := service.Diff(ctx, metadata, diffRequest(baselineA, nil))
	if err != nil || seed.Response.GetServerVersion() != 0 {
		t.Fatalf("seed diff: result=%+v err=%v", seed, err)
	}
	mutations := []*syncv1.Mutation{
		mutation("operation_alpha_0001", "stella/v1/day/2026-07-13/journal", `"alpha"`),
		mutation("operation_beta_00002", "stella/v1/day/2026-07-14/journal", `"beta"`),
	}
	results := make(chan *syncservice.DiffResult, 2)
	errorsChannel := make(chan error, 2)
	var group sync.WaitGroup
	for _, item := range mutations {
		item := item
		group.Add(1)
		go func() {
			defer group.Done()
			result, callErr := service.Diff(ctx, metadata, diffRequest(baselineA, []*syncv1.Mutation{item}))
			if callErr != nil {
				errorsChannel <- callErr
				return
			}
			results <- result
		}()
	}
	group.Wait()
	close(errorsChannel)
	for callErr := range errorsChannel {
		t.Fatal(callErr)
	}
	close(results)
	versions := map[uint64]bool{}
	for result := range results {
		versions[result.ServerVersion] = true
	}
	if !versions[1] || !versions[2] {
		t.Fatalf("concurrent batches did not serialize versions once each: %v", versions)
	}

	replay, err := service.Diff(ctx, metadata, diffRequest(baselineA, []*syncv1.Mutation{mutations[0]}))
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Response.GetAcks()) != 1 || !replay.Response.GetAcks()[0].GetApplied() || replay.StateChanged {
		t.Fatalf("opId replay changed state: %+v", replay)
	}
	if len(replay.Response.GetCanonicalChanges()) != 1 || replay.Response.GetCanonicalChanges()[0].GetOpId() != mutations[0].GetOpId() {
		t.Fatalf("opId replay did not return its canonical operation: %+v", replay.Response)
	}
	feed, err := service.ReadFeedPage(ctx, owner, baselineA, 0, 1)
	if err != nil || !feed.HasMore || len(feed.Changes) != 1 || feed.NextCursor != 1 || feed.HeadCursor != 2 {
		t.Fatalf("feed limit=1 did not paginate two operations: page=%+v err=%v", feed, err)
	}

	conflicting := mutation("operation_conflict_01", mutations[0].GetEntityKey(), `"conflict"`)
	conflict, err := service.Diff(ctx, metadata, diffRequest(baselineA, []*syncv1.Mutation{conflicting}))
	if err != nil {
		t.Fatal(err)
	}
	ack := conflict.Response.GetAcks()[0]
	if !ack.GetConflict() || ack.GetApplied() || conflict.StateChanged {
		t.Fatalf("unexpected conflict ack: %+v", ack)
	}

	reset, err := service.ReadFeedPage(ctx, owner, baselineA, 999, 128)
	if err != nil || !reset.ResetRequired {
		t.Fatalf("cursor reset was not requested: result=%+v err=%v", reset, err)
	}
	mismatch, err := service.Diff(ctx, metadata, diffRequest(baselineB, nil))
	if err != nil || !mismatch.Response.GetBaselineMismatch() {
		t.Fatalf("baseline mismatch was not returned: result=%+v err=%v", mismatch, err)
	}

	stale, err := service.ResolveBaseline(ctx, metadata, &syncv1.ResolveBaselineRequest{
		RequestId: "resolve_stale_0001", DeviceId: "device_alpha", LocalBaselineId: baselineB,
		ExpectedServerBaselineId: baselineA, ExpectedServerVersion: 1,
		Choice: syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
	})
	if err != nil || !stale.Response.GetStale() {
		t.Fatalf("stale CAS was not rejected: result=%+v err=%v", stale, err)
	}

	resolved, err := service.ResolveBaseline(ctx, metadata, &syncv1.ResolveBaselineRequest{
		RequestId: "resolve_local_0001", DeviceId: "device_alpha", LocalBaselineId: baselineB,
		ExpectedServerBaselineId: baselineA, ExpectedServerVersion: 2,
		Choice: syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
		LocalSnapshot: []*syncv1.Mutation{
			mutation("snapshot_operation_01", "stella/v1/day/2026-07-15/journal", `"local"`),
		},
	})
	if err != nil || resolved.Response.GetStale() || resolved.Response.GetBaselineId() != baselineB || resolved.Response.GetServerVersion() != 1 {
		t.Fatalf("USE_LOCAL failed: result=%+v err=%v", resolved, err)
	}
	var archiveA, archiveB int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_archive_changes WHERE owner_key = ? AND baseline_id = ?", owner, baselineA).Scan(&archiveA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_archive_changes WHERE owner_key = ? AND baseline_id = ?", owner, baselineB).Scan(&archiveB); err != nil {
		t.Fatal(err)
	}
	if archiveA != 2 || archiveB != 1 {
		t.Fatalf("cross-baseline archive was not preserved: A=%d B=%d", archiveA, archiveB)
	}

	restored, err := service.ResolveBaseline(ctx, metadata, &syncv1.ResolveBaselineRequest{
		RequestId: "resolve_restore_a_01", DeviceId: "device_alpha", LocalBaselineId: baselineA,
		ExpectedServerBaselineId: baselineB, ExpectedServerVersion: 1,
		Choice: syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
		LocalSnapshot: []*syncv1.Mutation{
			mutation("snapshot_operation_a2", "stella/v1/day/2026-07-16/journal", `"restored"`),
		},
	})
	if err != nil || restored.Response.GetStale() || restored.Response.GetBaselineId() != baselineC ||
		restored.Response.GetServerVersion() != 1 {
		t.Fatalf("restoring archived baseline did not fork a new lineage: result=%+v err=%v", restored, err)
	}
	var archiveC int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_archive_changes WHERE owner_key = ? AND baseline_id = ?", owner, baselineC).Scan(&archiveC); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_archive_changes WHERE owner_key = ? AND baseline_id = ?", owner, baselineA).Scan(&archiveA); err != nil {
		t.Fatal(err)
	}
	if archiveA != 2 || archiveC != 1 {
		t.Fatalf("restored lineage archive mismatch: A=%d C=%d", archiveA, archiveC)
	}
}

func TestRedisSharedLimitsLeasesPubSubHandoffAndOutbox(t *testing.T) {
	redisURL := os.Getenv("REDIS_TEST_URL")
	if redisURL == "" {
		t.Skip("REDIS_TEST_URL is not set")
	}
	prefix := fmt.Sprintf("study-list:integration:%d:", time.Now().UnixNano())
	first, err := realtime.New(redisURL, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := realtime.New(redisURL, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for index := 0; index < 8; index++ {
		retry, err := first.CheckFixedWindow(ctx, "exchange", "owner", 8, 10*time.Second)
		if err != nil || retry != 0 {
			t.Fatalf("rate request %d: retry=%s err=%v", index, retry, err)
		}
	}
	if retry, err := second.CheckFixedWindow(ctx, "exchange", "owner", 8, 10*time.Second); err != nil || retry <= 0 {
		t.Fatalf("shared rate limit did not reject ninth request: retry=%s err=%v", retry, err)
	}
	for index := 0; index < 8; index++ {
		if err := first.AcquireConnection(ctx, "owner", fmt.Sprintf("connection-%d", index), 8, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := second.AcquireConnection(ctx, "owner", "connection-9", 8, time.Second); !errors.Is(err, realtime.ErrConnectionLimit) {
		t.Fatalf("ninth connection returned %v", err)
	}
	if err := first.AcquireConnection(ctx, "expiring", "old", 1, 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := second.AcquireConnection(ctx, "expiring", "new", 1, time.Second); err != nil {
		t.Fatalf("expired lease was not removed: %v", err)
	}

	hints := make(chan realtime.Hint, 2)
	subscriptionContext, subscriptionCancel := context.WithCancel(ctx)
	defer subscriptionCancel()
	go func() { _ = second.SubscribeHints(subscriptionContext, func(hint realtime.Hint) { hints <- hint }) }()
	time.Sleep(100 * time.Millisecond)
	expected := realtime.Hint{OwnerKey: "owner", BaselineID: baselineA, ServerCursor: 3, ServerVersion: 2, OriginConnectionID: "writer"}
	if err := first.PublishHint(ctx, expected); err != nil {
		t.Fatal(err)
	}
	select {
	case received := <-hints:
		if received != expected {
			t.Fatalf("unexpected cross-instance hint: %+v", received)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cross-instance hint")
	}
	db := integrationDB(t)
	store := mysqlstore.New(db)
	service := syncservice.NewService(store)
	owner := auth.OwnerKey(fmt.Sprintf("outbox-%d", time.Now().UnixNano()))
	_, err = service.Exchange(ctx, syncservice.CommandMetadata{OwnerKey: owner, OriginConnectionID: "writer"},
		request(baselineA, 0, []*syncv1.Mutation{mutation("outbox_operation_01", "stella/v1/day/2026-07-13/journal", `"outbox"`)}, 128))
	if err != nil {
		t.Fatal(err)
	}
	outboxContext, outboxCancel := context.WithCancel(ctx)
	defer outboxCancel()
	flaky := &flakyPublisher{delegate: first}
	go func() { _ = mysqlstore.RunOutbox(outboxContext, db, flaky, nil) }()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case received := <-hints:
			if received.OwnerKey != owner {
				continue
			}
			if received.OriginConnectionID != "writer" {
				t.Fatalf("outbox lost origin connection: %+v", received)
			}
			commitDeadline := time.Now().Add(3 * time.Second)
			for {
				var attempts uint64
				var publishedAt sql.NullInt64
				if err := db.QueryRowContext(ctx,
					"SELECT publish_attempts, published_at_ms FROM realtime_outbox WHERE owner_key = ? ORDER BY id DESC LIMIT 1", owner,
				).Scan(&attempts, &publishedAt); err != nil {
					t.Fatal(err)
				}
				if attempts >= 2 && publishedAt.Valid {
					return
				}
				if time.Now().After(commitDeadline) {
					t.Fatalf("outbox did not retry and mark published: attempts=%d published=%v", attempts, publishedAt.Valid)
				}
				time.Sleep(20 * time.Millisecond)
			}
		case <-deadline:
			t.Fatal("timed out waiting for outbox publication")
		}
	}
}

func request(baseline string, cursor uint64, mutations []*syncv1.Mutation, limit uint32) *syncv1.SyncRequest {
	return &syncv1.SyncRequest{
		DeviceId: "device_alpha", Cursor: cursor, Mutations: mutations, PullLimit: limit,
		BaselineId: baseline, LocalProgressDay: "2026-07-13",
	}
}

func diffRequest(baseline string, mutations []*syncv1.Mutation) *syncv1.DiffRequest {
	return &syncv1.DiffRequest{
		DeviceId: "device_alpha", Mutations: mutations, BaselineId: baseline,
		LocalProgressDay: "2026-07-13",
	}
}

func mutation(opID, entityKey, value string) *syncv1.Mutation {
	return &syncv1.Mutation{
		OpId: opID, EntityKey: entityKey, ClientTimeMs: uint64(time.Now().UnixMilli()),
		ValueJson: []byte(value), DeviceId: "device_alpha", ClientSeq: 1,
	}
}

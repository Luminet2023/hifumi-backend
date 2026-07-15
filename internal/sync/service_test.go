package sync

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"google.golang.org/protobuf/proto"
)

const (
	testOwner     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testBaselineA = "baseline_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testBaselineB = "baseline_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testBaselineC = "baseline_cccccccccccccccccccccccccccccccc"
)

func TestExchangeBatchReplayConflictAndBaselineMismatch(t *testing.T) {
	store := newMemoryStore()
	service := newTestService(store)
	request := &syncv1.SyncRequest{
		DeviceId:   "device_alpha",
		BaselineId: testBaselineA,
		PullLimit:  128,
		Mutations: []*syncv1.Mutation{
			testMutation("operation_alpha_0001", "stella/v1/day/2026-07-15/journal", "device_alpha", 0, `"done"`),
			testMutation("operation_alpha_0002", "stella/v1/preference/fontFamily", "device_alpha", 0, `"system"`),
		},
	}

	first, err := service.Exchange(context.Background(), CommandMetadata{
		OwnerKey: testOwner, OriginConnectionID: "connection_writer",
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	if !first.StateChanged || first.ServerCursor != 2 || first.ServerVersion != 1 {
		t.Fatalf("unexpected first result: %+v", first)
	}
	if first.Response.GetServerProgressDay() != "2026-07-15" {
		t.Fatalf("progress day = %q", first.Response.GetServerProgressDay())
	}
	if len(first.Response.GetAcks()) != 2 || !first.Response.GetAcks()[0].GetApplied() {
		t.Fatalf("unexpected acks: %+v", first.Response.GetAcks())
	}
	state := store.owners[testOwner]
	if len(state.archives) != 2 || len(state.events) != 1 {
		t.Fatalf("archive/outbox mismatch: archives=%d events=%d", len(state.archives), len(state.events))
	}
	if state.events[0].OriginConnectionID != "connection_writer" {
		t.Fatalf("origin connection lost: %+v", state.events[0])
	}

	replay, err := service.Exchange(context.Background(), CommandMetadata{OwnerKey: testOwner}, request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.StateChanged || replay.ServerCursor != 2 || replay.ServerVersion != 1 {
		t.Fatalf("replay advanced state: %+v", replay)
	}
	if !proto.Equal(first.Response.GetAcks()[0], replay.Response.GetAcks()[0]) {
		t.Fatalf("replay ack changed: first=%v replay=%v", first.Response.GetAcks()[0], replay.Response.GetAcks()[0])
	}
	if len(state.events) != 1 {
		t.Fatalf("replay emitted outbox event: %d", len(state.events))
	}

	conflict, err := service.Exchange(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.SyncRequest{
		DeviceId:   "device_beta",
		BaselineId: testBaselineA,
		Cursor:     2,
		PullLimit:  128,
		Mutations: []*syncv1.Mutation{
			testMutation("operation_conflict_01", "stella/v1/day/2026-07-15/journal", "device_beta", 0, `"stale"`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ack := conflict.Response.GetAcks()[0]
	if ack.GetApplied() || !ack.GetConflict() || ack.GetServerCursor() != 1 {
		t.Fatalf("unexpected conflict ack: %+v", ack)
	}
	if len(conflict.Response.GetChanges()) != 1 || conflict.Response.GetChanges()[0].GetOpId() != "operation_alpha_0001" {
		t.Fatalf("server record not forced into conflict response: %+v", conflict.Response.GetChanges())
	}
	if conflict.ServerVersion != 1 || conflict.StateChanged {
		t.Fatalf("conflict advanced logical version: %+v", conflict)
	}

	mismatch, err := service.Exchange(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.SyncRequest{
		DeviceId:   "device_beta",
		BaselineId: testBaselineB,
		Mutations: []*syncv1.Mutation{
			testMutation("operation_wrong_base", "stella/v1/day/2026-07-16/journal", "device_beta", 0, `"ignored"`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !mismatch.Response.GetBaselineMismatch() || mismatch.Response.GetBaselineId() != testBaselineA {
		t.Fatalf("baseline mismatch not reported: %+v", mismatch.Response)
	}
	if mismatch.StateChanged || mismatch.ServerCursor != 2 || len(state.records) != 2 {
		t.Fatalf("baseline mismatch mutated state: %+v", mismatch)
	}
}

func TestExchangePaginationAndReset(t *testing.T) {
	store := newMemoryStore()
	service := newTestService(store)
	mutations := []*syncv1.Mutation{
		testMutation("operation_page_0001", "stella/v1/day/2026-07-13/journal", "device_alpha", 0, `"one"`),
		testMutation("operation_page_0002", "stella/v1/preference/fontFamily", "device_alpha", 0, `"two"`),
		testMutation("operation_page_0003", "stella/v1/day/2026-07-13/blessing", "device_alpha", 0, `{"liked":true}`),
	}
	if _, err := service.Exchange(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.SyncRequest{
		DeviceId: "device_alpha", BaselineId: testBaselineA, Mutations: mutations,
	}); err != nil {
		t.Fatal(err)
	}

	for cursor, wantNext := range []uint64{1, 2, 3} {
		response, err := service.Exchange(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.SyncRequest{
			DeviceId: "device_beta", BaselineId: testBaselineA, Cursor: uint64(cursor), PullLimit: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.Response.GetNextCursor() != wantNext {
			t.Fatalf("cursor %d next=%d want=%d", cursor, response.Response.GetNextCursor(), wantNext)
		}
		wantMore := wantNext < 3
		if response.Response.GetHasMore() != wantMore {
			t.Fatalf("cursor %d hasMore=%v want=%v", cursor, response.Response.GetHasMore(), wantMore)
		}
	}

	reset, err := service.Exchange(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.SyncRequest{
		DeviceId: "device_beta", BaselineId: testBaselineA, Cursor: 99, PullLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reset.Response.GetResetRequired() || len(reset.Response.GetChanges()) != 0 || reset.ServerVersion != 1 {
		t.Fatalf("unexpected reset response: %+v", reset.Response)
	}
}

func TestResolveBaselineCASUseLocalReplayAndEmptySnapshot(t *testing.T) {
	store := newMemoryStore()
	service := newTestService(store)
	if _, err := service.Exchange(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.SyncRequest{
		DeviceId: "device_alpha", BaselineId: testBaselineA,
		Mutations: []*syncv1.Mutation{
			testMutation("operation_server_old", "stella/v1/day/2026-07-13/journal", "device_alpha", 0, `"server"`),
		},
	}); err != nil {
		t.Fatal(err)
	}

	request := &syncv1.ResolveBaselineRequest{
		RequestId:                "resolution_local_0001",
		DeviceId:                 "device_alpha",
		LocalBaselineId:          testBaselineB,
		ExpectedServerBaselineId: testBaselineA,
		ExpectedServerVersion:    1,
		Choice:                   syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
		LocalSnapshot: []*syncv1.Mutation{
			testMutation("operation_local_new", "stella/v1/day/2026-07-16/journal", "device_alpha", 0, `"local"`),
		},
	}
	first, err := service.ResolveBaseline(context.Background(), CommandMetadata{
		OwnerKey: testOwner, OriginConnectionID: "connection_resolver",
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	if !first.StateChanged || first.BaselineID != testBaselineB || first.ServerCursor != 1 || first.ServerVersion != 1 {
		t.Fatalf("unexpected USE_LOCAL result: %+v", first)
	}
	if first.Response.GetServerProgressDay() != "2026-07-16" || len(first.Response.GetRecords()) != 1 {
		t.Fatalf("unexpected USE_LOCAL snapshot: %+v", first.Response)
	}

	replay, err := service.ResolveBaseline(context.Background(), CommandMetadata{OwnerKey: testOwner}, request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.StateChanged || !proto.Equal(first.Response, replay.Response) {
		t.Fatalf("resolution replay changed result: first=%v replay=%v", first.Response, replay.Response)
	}

	stale, err := service.ResolveBaseline(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.ResolveBaselineRequest{
		RequestId:                "resolution_stale_0002",
		DeviceId:                 "device_alpha",
		LocalBaselineId:          testBaselineC,
		ExpectedServerBaselineId: testBaselineB,
		ExpectedServerVersion:    0,
		Choice:                   syncv1.BaselineChoice_BASELINE_CHOICE_USE_SERVER,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !stale.Response.GetStale() || len(stale.Response.GetRecords()) != 0 || stale.BaselineID != testBaselineB {
		t.Fatalf("stale CAS not reported: %+v", stale.Response)
	}

	empty, err := service.ResolveBaseline(context.Background(), CommandMetadata{OwnerKey: testOwner}, &syncv1.ResolveBaselineRequest{
		RequestId:                "resolution_empty_0003",
		DeviceId:                 "device_alpha",
		LocalBaselineId:          testBaselineC,
		ExpectedServerBaselineId: testBaselineB,
		ExpectedServerVersion:    1,
		Choice:                   syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if empty.ServerVersion != 0 || empty.ServerCursor != 0 || len(empty.Response.GetRecords()) != 0 ||
		empty.Response.GetServerProgressDay() != campaignStart {
		t.Fatalf("empty baseline semantics changed: %+v", empty.Response)
	}
}

func TestResolveBaselineUseLocalForksPreviouslyArchivedBaseline(t *testing.T) {
	store := newMemoryStore()
	service := newTestService(store)
	ctx := context.Background()
	metadata := CommandMetadata{OwnerKey: testOwner, OriginConnectionID: "connection_resolver"}

	if _, err := service.Exchange(ctx, metadata, &syncv1.SyncRequest{
		DeviceId: "device_alpha", BaselineId: testBaselineA,
		Mutations: []*syncv1.Mutation{
			testMutation("operation_original_a", "stella/v1/day/2026-07-13/journal", "device_alpha", 0, `"original A"`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ResolveBaseline(ctx, metadata, &syncv1.ResolveBaselineRequest{
		RequestId:                "resolution_switch_to_b",
		DeviceId:                 "device_beta",
		LocalBaselineId:          testBaselineB,
		ExpectedServerBaselineId: testBaselineA,
		ExpectedServerVersion:    1,
		Choice:                   syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
		LocalSnapshot: []*syncv1.Mutation{
			testMutation("operation_snapshot_b", "stella/v1/day/2026-07-14/journal", "device_beta", 0, `"snapshot B"`),
		},
	}); err != nil {
		t.Fatal(err)
	}

	restoreRequest := &syncv1.ResolveBaselineRequest{
		RequestId:                "resolution_restore_a",
		DeviceId:                 "device_alpha",
		LocalBaselineId:          testBaselineA,
		ExpectedServerBaselineId: testBaselineB,
		ExpectedServerVersion:    1,
		Choice:                   syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL,
		LocalSnapshot: []*syncv1.Mutation{
			testMutation("operation_restored_a", "stella/v1/day/2026-07-15/journal", "device_alpha", 0, `"restored A"`),
		},
	}
	result, err := service.ResolveBaseline(ctx, metadata, restoreRequest)
	if err != nil {
		t.Fatal(err)
	}
	if result.BaselineID != testBaselineC || result.Response.GetBaselineId() != testBaselineC {
		t.Fatalf("restored archived baseline was not forked: %+v", result)
	}
	if result.ServerCursor != 1 || result.ServerVersion != 1 || len(result.Response.GetRecords()) != 1 {
		t.Fatalf("unexpected restored snapshot result: %+v", result.Response)
	}
	if _, exists := store.owners[testOwner].archives[testBaselineA+":"+strings.Repeat("0", 15)+"1"]; !exists {
		t.Fatal("original baseline archive was lost")
	}
	replay, err := service.ResolveBaseline(ctx, metadata, restoreRequest)
	if err != nil {
		t.Fatal(err)
	}
	if replay.StateChanged || !proto.Equal(result.Response, replay.Response) {
		t.Fatalf("forked resolution replay changed result: first=%v replay=%v", result.Response, replay.Response)
	}
}

func TestResolutionRequestIDReuseWithDifferentChoiceFails(t *testing.T) {
	store := newMemoryStore()
	service := newTestService(store)
	base := &syncv1.ResolveBaselineRequest{
		RequestId:                "resolution_reuse_0001",
		DeviceId:                 "device_alpha",
		LocalBaselineId:          testBaselineB,
		ExpectedServerBaselineId: testBaselineB,
		Choice:                   syncv1.BaselineChoice_BASELINE_CHOICE_USE_SERVER,
	}
	if _, err := service.ResolveBaseline(context.Background(), CommandMetadata{OwnerKey: testOwner}, base); err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(base).(*syncv1.ResolveBaselineRequest)
	changed.Choice = syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL
	_, err := service.ResolveBaseline(context.Background(), CommandMetadata{OwnerKey: testOwner}, changed)
	var operationError *OperationError
	if !errors.As(err, &operationError) || operationError.Code != CodeFailedPrecondition {
		t.Fatalf("got error %v, want FAILED_PRECONDITION", err)
	}
}

func newTestService(store Store) *Service {
	fixed := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	return NewService(store,
		WithClock(func() time.Time { return fixed }),
		WithBaselineIDGenerator(func() (string, error) { return testBaselineC, nil }),
	)
}

func testMutation(opID, entityKey, deviceID string, baseVersion uint64, value string) *syncv1.Mutation {
	return &syncv1.Mutation{
		OpId: opID, EntityKey: entityKey, DeviceId: deviceID, BaseVersion: baseVersion,
		ClientTimeMs: 1_789_000_000_000, ValueJson: []byte(value), ClientSeq: 1,
	}
}

type memoryStore struct {
	owners map[string]*memoryTx
}

func newMemoryStore() *memoryStore {
	return &memoryStore{owners: make(map[string]*memoryTx)}
}

func (s *memoryStore) WithOwnerTransaction(_ context.Context, ownerKey string, fn func(Tx) error) error {
	state := s.owners[ownerKey]
	if state == nil {
		state = newMemoryTx()
		s.owners[ownerKey] = state
	}
	return fn(state)
}

type memoryTx struct {
	cursor      uint64
	lineage     *Lineage
	records     map[string]*syncv1.Change
	operations  map[uint64]*syncv1.Change
	receipts    map[string]Receipt
	resolutions map[string]ResolutionReceipt
	archives    map[string]*syncv1.Change
	heads       map[string]uint64
	events      []RealtimeEvent
}

func newMemoryTx() *memoryTx {
	return &memoryTx{
		records: make(map[string]*syncv1.Change), operations: make(map[uint64]*syncv1.Change),
		receipts: make(map[string]Receipt), resolutions: make(map[string]ResolutionReceipt),
		archives: make(map[string]*syncv1.Change), heads: make(map[string]uint64),
	}
}

func (t *memoryTx) CurrentCursor(context.Context) (uint64, error) { return t.cursor, nil }
func (t *memoryTx) NextCursor(context.Context) (uint64, error) {
	t.cursor++
	return t.cursor, nil
}
func (t *memoryTx) GetLineage(context.Context) (Lineage, error) {
	if t.lineage == nil {
		return Lineage{}, ErrNotFound
	}
	return *t.lineage, nil
}
func (t *memoryTx) InsertLineage(_ context.Context, lineage Lineage) error {
	t.lineage = &lineage
	return nil
}
func (t *memoryTx) SetLineage(_ context.Context, lineage Lineage) error {
	t.lineage = &lineage
	return nil
}
func (t *memoryTx) IncrementLineage(_ context.Context, updatedAtMs uint64, progressDay string) (Lineage, error) {
	t.lineage.Version++
	t.lineage.UpdatedAtMs = updatedAtMs
	t.lineage.ProgressDay = progressDay
	return *t.lineage, nil
}
func (t *memoryTx) GetRecord(_ context.Context, entityKey string) (*syncv1.Change, error) {
	change := t.records[entityKey]
	if change == nil {
		return nil, ErrNotFound
	}
	return cloneChange(change), nil
}
func (t *memoryTx) ListRecords(context.Context) ([]*syncv1.Change, error) {
	keys := make([]string, 0, len(t.records))
	for key := range t.records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	changes := make([]*syncv1.Change, 0, len(keys))
	for _, key := range keys {
		changes = append(changes, cloneChange(t.records[key]))
	}
	return changes, nil
}
func (t *memoryTx) PullOperations(_ context.Context, afterCursor uint64, limit int) ([]*syncv1.Change, error) {
	changes := make([]*syncv1.Change, 0)
	for cursor := afterCursor + 1; cursor <= t.cursor && len(changes) < limit; cursor++ {
		if change := t.operations[cursor]; change != nil {
			changes = append(changes, cloneChange(change))
		}
	}
	return changes, nil
}
func (t *memoryTx) AppendOperation(_ context.Context, change *syncv1.Change) error {
	t.operations[change.GetCursor()] = cloneChange(change)
	return nil
}
func (t *memoryTx) UpsertRecord(_ context.Context, change *syncv1.Change) error {
	t.records[change.GetEntityKey()] = cloneChange(change)
	return nil
}
func (t *memoryTx) GetReceipt(_ context.Context, opID string) (Receipt, error) {
	receipt, exists := t.receipts[opID]
	if !exists {
		return Receipt{}, ErrNotFound
	}
	return receipt, nil
}
func (t *memoryTx) InsertReceipt(_ context.Context, receipt Receipt) error {
	t.receipts[receipt.OpID] = receipt
	return nil
}
func (t *memoryTx) GetResolutionReceipt(_ context.Context, requestID string) (ResolutionReceipt, error) {
	receipt, exists := t.resolutions[requestID]
	if !exists {
		return ResolutionReceipt{}, ErrNotFound
	}
	return receipt, nil
}
func (t *memoryTx) InsertResolutionReceipt(_ context.Context, receipt ResolutionReceipt) error {
	t.resolutions[receipt.RequestID] = receipt
	return nil
}
func (t *memoryTx) ClearCurrentBaseline(context.Context) error {
	t.cursor = 0
	t.records = make(map[string]*syncv1.Change)
	t.operations = make(map[uint64]*syncv1.Change)
	t.receipts = make(map[string]Receipt)
	t.resolutions = make(map[string]ResolutionReceipt)
	return nil
}
func (t *memoryTx) GetArchiveHead(_ context.Context, baselineID string) (ArchiveHead, error) {
	cursor, exists := t.heads[baselineID]
	if !exists {
		return ArchiveHead{}, ErrNotFound
	}
	return ArchiveHead{Cursor: cursor}, nil
}
func (t *memoryTx) ArchiveChange(_ context.Context, baselineID string, _ uint64, _ uint64, change *syncv1.Change) error {
	key := baselineID + ":" + strings.Repeat("0", 16-lenUint(change.GetCursor())) + uintString(change.GetCursor())
	if _, exists := t.archives[key]; exists {
		return errors.New("duplicate archive key")
	}
	t.archives[key] = cloneChange(change)
	return nil
}
func (t *memoryTx) UpsertArchiveHead(_ context.Context, baselineID string, cursor uint64, _ uint64, _ uint64) error {
	t.heads[baselineID] = cursor
	return nil
}
func (t *memoryTx) InsertRealtimeEvent(_ context.Context, event RealtimeEvent) error {
	t.events = append(t.events, event)
	return nil
}

func cloneChange(change *syncv1.Change) *syncv1.Change {
	return proto.Clone(change).(*syncv1.Change)
}

func lenUint(value uint64) int { return len(uintString(value)) }
func uintString(value uint64) string {
	if value == 0 {
		return "0"
	}
	buffer := make([]byte, 0, 20)
	for value > 0 {
		buffer = append(buffer, byte('0'+value%10))
		value /= 10
	}
	for left, right := 0, len(buffer)-1; left < right; left, right = left+1, right-1 {
		buffer[left], buffer[right] = buffer[right], buffer[left]
	}
	return string(buffer)
}

var _ Store = (*memoryStore)(nil)
var _ Tx = (*memoryTx)(nil)

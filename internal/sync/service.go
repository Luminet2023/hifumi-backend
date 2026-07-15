package sync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
	"google.golang.org/protobuf/proto"
)

const (
	maxFutureSkewMs      = uint64(5 * time.Minute / time.Millisecond)
	maxDiffResponseBytes = 512 * 1024
)

type CommandMetadata struct {
	OwnerKey           string
	OriginConnectionID string
}

type ExchangeResult struct {
	Response      *syncv1.SyncResponse
	StateChanged  bool
	DeviceID      string
	BaselineID    string
	ServerCursor  uint64
	ServerVersion uint64
}

type DiffResult struct {
	Response      *syncv1.DiffResponse
	StateChanged  bool
	DeviceID      string
	BaselineID    string
	ServerCursor  uint64
	ServerVersion uint64
}

type FeedPage struct {
	Changes           []*syncv1.Change
	NextCursor        uint64
	HasMore           bool
	BaselineID        string
	HeadCursor        uint64
	ServerVersion     uint64
	ServerUpdatedAtMs uint64
	ServerProgressDay string
	BaselineMismatch  bool
	ResetRequired     bool
}

type ResolveResult struct {
	Response      *syncv1.ResolveBaselineResponse
	StateChanged  bool
	DeviceID      string
	BaselineID    string
	ServerCursor  uint64
	ServerVersion uint64
}

type Service struct {
	store             Store
	now               func() time.Time
	newBaselineIDFunc func() (string, error)
}

type Option func(*Service)

func WithClock(now func() time.Time) Option {
	return func(service *Service) { service.now = now }
}

func WithBaselineIDGenerator(generator func() (string, error)) Option {
	return func(service *Service) { service.newBaselineIDFunc = generator }
}

func NewService(store Store, options ...Option) *Service {
	service := &Service{
		store:             store,
		now:               time.Now,
		newBaselineIDFunc: randomBaselineID,
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) Exchange(
	ctx context.Context,
	metadata CommandMetadata,
	request *syncv1.SyncRequest,
) (*ExchangeResult, error) {
	if err := validateExchangeRequest(metadata.OwnerKey, request); err != nil {
		return nil, err
	}
	now := uint64(s.now().UnixMilli())
	var result *ExchangeResult
	err := s.store.WithOwnerTransaction(ctx, metadata.OwnerKey, func(tx Tx) error {
		lineage, err := s.ensureLineage(ctx, tx, request.GetBaselineId(), now)
		if err != nil {
			return err
		}
		headCursor, err := tx.CurrentCursor(ctx)
		if err != nil {
			return err
		}
		if lineage.BaselineID != request.GetBaselineId() {
			result = &ExchangeResult{
				Response: &syncv1.SyncResponse{
					NextCursor:        request.GetCursor(),
					BaselineId:        lineage.BaselineID,
					ServerVersion:     lineage.Version,
					ServerUpdatedAtMs: lineage.UpdatedAtMs,
					ServerProgressDay: lineage.ProgressDay,
					BaselineMismatch:  true,
				},
				DeviceID:      request.GetDeviceId(),
				BaselineID:    lineage.BaselineID,
				ServerCursor:  headCursor,
				ServerVersion: lineage.Version,
			}
			return nil
		}

		batch, err := applyMutations(ctx, tx, request.GetDeviceId(), request.GetMutations(), now)
		if err != nil {
			return err
		}

		lineage, headCursor, err = finalizeMutationBatch(ctx, tx, metadata, lineage, batch, now)
		if err != nil {
			return err
		}

		pullLimit := normalizedPullLimit(request.GetPullLimit())
		rows, err := tx.PullOperations(ctx, request.GetCursor(), pullLimit+1)
		if err != nil {
			return err
		}
		hasMore := len(rows) > pullLimit
		pageChanges := rows
		if hasMore {
			pageChanges = rows[:pullLimit]
		}
		changes := append([]*syncv1.Change(nil), pageChanges...)
		seen := make(map[string]struct{}, len(pageChanges))
		for _, change := range pageChanges {
			seen[changeIdentity(change)] = struct{}{}
		}
		for _, change := range batch.forcedChanges {
			// 旧实现没有在追加 forced change 后更新 seen；这里有意保持该细节。
			if _, exists := seen[changeIdentity(change)]; !exists {
				changes = append(changes, change)
			}
		}
		sort.SliceStable(changes, func(left, right int) bool {
			return changes[left].GetCursor() < changes[right].GetCursor()
		})
		nextCursor := request.GetCursor()
		if len(pageChanges) > 0 {
			nextCursor = pageChanges[len(pageChanges)-1].GetCursor()
		}
		result = &ExchangeResult{
			Response: &syncv1.SyncResponse{
				NextCursor:        nextCursor,
				Acks:              batch.acks,
				Changes:           changes,
				HasMore:           hasMore,
				ResetRequired:     request.GetCursor() > headCursor,
				BaselineId:        lineage.BaselineID,
				ServerVersion:     lineage.Version,
				ServerUpdatedAtMs: lineage.UpdatedAtMs,
				ServerProgressDay: lineage.ProgressDay,
			},
			StateChanged:  batch.appliedCount > 0,
			DeviceID:      request.GetDeviceId(),
			BaselineID:    lineage.BaselineID,
			ServerCursor:  headCursor,
			ServerVersion: lineage.Version,
		}
		return nil
	})
	return result, err
}

func (s *Service) Diff(
	ctx context.Context,
	metadata CommandMetadata,
	request *syncv1.DiffRequest,
) (*DiffResult, error) {
	if err := validateDiffRequest(metadata.OwnerKey, request); err != nil {
		return nil, err
	}
	now := uint64(s.now().UnixMilli())
	var result *DiffResult
	err := s.store.WithOwnerTransaction(ctx, metadata.OwnerKey, func(tx Tx) error {
		lineage, err := s.ensureLineage(ctx, tx, request.GetBaselineId(), now)
		if err != nil {
			return err
		}
		headCursor, err := tx.CurrentCursor(ctx)
		if err != nil {
			return err
		}
		if lineage.BaselineID != request.GetBaselineId() {
			response := &syncv1.DiffResponse{
				BaselineId:        lineage.BaselineID,
				ServerCursor:      headCursor,
				ServerVersion:     lineage.Version,
				ServerUpdatedAtMs: lineage.UpdatedAtMs,
				ServerProgressDay: lineage.ProgressDay,
				BaselineMismatch:  true,
			}
			if err := validateDiffResponseSize(response); err != nil {
				return err
			}
			result = &DiffResult{
				Response:      response,
				DeviceID:      request.GetDeviceId(),
				BaselineID:    lineage.BaselineID,
				ServerCursor:  headCursor,
				ServerVersion: lineage.Version,
			}
			return nil
		}

		batch, err := applyMutations(ctx, tx, request.GetDeviceId(), request.GetMutations(), now)
		if err != nil {
			return err
		}
		lineage, headCursor, err = finalizeMutationBatch(ctx, tx, metadata, lineage, batch, now)
		if err != nil {
			return err
		}
		response := &syncv1.DiffResponse{
			Acks:              batch.acks,
			CanonicalChanges:  batch.canonicalChanges,
			BaselineId:        lineage.BaselineID,
			ServerCursor:      headCursor,
			ServerVersion:     lineage.Version,
			ServerUpdatedAtMs: lineage.UpdatedAtMs,
			ServerProgressDay: lineage.ProgressDay,
		}
		// 必须在事务提交前做 deterministic protobuf 上限检查；返回错误会让
		// WithOwnerTransaction 回滚 mutations、receipts、archive 和 realtime outbox。
		if err := validateDiffResponseSize(response); err != nil {
			return err
		}
		result = &DiffResult{
			Response:      response,
			StateChanged:  batch.appliedCount > 0,
			DeviceID:      request.GetDeviceId(),
			BaselineID:    lineage.BaselineID,
			ServerCursor:  headCursor,
			ServerVersion: lineage.Version,
		}
		return nil
	})
	return result, err
}

func (s *Service) ReadFeedPage(
	ctx context.Context,
	ownerKey string,
	baselineID string,
	cursor uint64,
	limit int,
) (*FeedPage, error) {
	if err := validateOwnerKey(ownerKey); err != nil {
		return nil, err
	}
	if err := validateBaselineID(baselineID, "baseline_id"); err != nil {
		return nil, err
	}
	if err := validateSafe(cursor); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, invalidArgument("invalid feed limit")
	}
	feedLimit := normalizedFeedLimit(limit)
	snapshot, err := s.store.ReadFeedSnapshot(ctx, ownerKey, cursor, feedLimit+1)
	if errors.Is(err, ErrNotFound) {
		return &FeedPage{
			NextCursor:        cursor,
			BaselineID:        baselineID,
			ServerProgressDay: campaignStart,
			ResetRequired:     cursor > 0,
		}, nil
	}
	if err != nil {
		return nil, err
	}

	page := &FeedPage{
		NextCursor:        cursor,
		BaselineID:        snapshot.Lineage.BaselineID,
		HeadCursor:        snapshot.HeadCursor,
		ServerVersion:     snapshot.Lineage.Version,
		ServerUpdatedAtMs: snapshot.Lineage.UpdatedAtMs,
		ServerProgressDay: snapshot.Lineage.ProgressDay,
		BaselineMismatch:  snapshot.Lineage.BaselineID != baselineID,
		ResetRequired:     cursor > snapshot.HeadCursor,
	}
	if page.BaselineMismatch || page.ResetRequired {
		return page, nil
	}
	page.HasMore = len(snapshot.Changes) > feedLimit
	page.Changes = snapshot.Changes
	if page.HasMore {
		page.Changes = page.Changes[:feedLimit]
	}
	if len(page.Changes) > 0 {
		page.NextCursor = page.Changes[len(page.Changes)-1].GetCursor()
	}
	return page, nil
}

func (s *Service) ResolveBaseline(
	ctx context.Context,
	metadata CommandMetadata,
	request *syncv1.ResolveBaselineRequest,
) (*ResolveResult, error) {
	if err := validateResolveEnvelope(metadata.OwnerKey, request); err != nil {
		return nil, err
	}
	now := uint64(s.now().UnixMilli())
	var result *ResolveResult
	err := s.store.WithOwnerTransaction(ctx, metadata.OwnerKey, func(tx Tx) error {
		lineage, err := s.ensureLineage(ctx, tx, request.GetLocalBaselineId(), now)
		if err != nil {
			return err
		}
		headCursor, err := tx.CurrentCursor(ctx)
		if err != nil {
			return err
		}

		receipt, err := tx.GetResolutionReceipt(ctx, request.GetRequestId())
		if err == nil {
			if !sameResolutionRequest(receipt, request) {
				return failedPrecondition("resolution request_id was reused with different data")
			}
			receiptIsCurrent := lineage.BaselineID == receipt.ResultBaselineID && lineage.Version == receipt.ResultVersion
			var records []*syncv1.Change
			if receiptIsCurrent {
				records, err = tx.ListRecords(ctx)
				if err != nil {
					return err
				}
			}
			result = resolveResult(request.GetDeviceId(), lineage, headCursor, records, !receiptIsCurrent, false)
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}

		if lineage.BaselineID != request.GetExpectedServerBaselineId() ||
			lineage.Version != request.GetExpectedServerVersion() {
			result = resolveResult(request.GetDeviceId(), lineage, headCursor, nil, true, false)
			return nil
		}

		if request.GetChoice() == syncv1.BaselineChoice_BASELINE_CHOICE_USE_LOCAL {
			if err := validateLocalSnapshot(request); err != nil {
				return err
			}
			resultBaselineID := request.GetLocalBaselineId()
			if _, err := tx.GetArchiveHead(ctx, resultBaselineID); err == nil {
				// 这个本地 baseline 曾经作为云端 lineage 使用过。若直接重用并把 cursor
				// 重置为 1，会与不可变归档的 (baseline_id, cursor) 主键冲突，也会让仍
				// 持有旧 cursor 的设备误以为这是同一轮 lineage。
				resultBaselineID, err = s.newBaselineIDFunc()
				if err != nil {
					return err
				}
			} else if !errors.Is(err, ErrNotFound) {
				return err
			}
			if err := tx.ClearCurrentBaseline(ctx); err != nil {
				return err
			}
			snapshot := append([]*syncv1.Mutation(nil), request.GetLocalSnapshot()...)
			sort.Slice(snapshot, func(left, right int) bool {
				return snapshot[left].GetEntityKey() < snapshot[right].GetEntityKey()
			})
			archiveChanges := make([]*syncv1.Change, 0, len(snapshot))
			for _, mutation := range snapshot {
				cursor, err := tx.NextCursor(ctx)
				if err != nil {
					return err
				}
				change := mutationChange(mutation, cursor, now)
				if err := tx.AppendOperation(ctx, change); err != nil {
					return err
				}
				if err := tx.UpsertRecord(ctx, change); err != nil {
					return err
				}
				archiveChanges = append(archiveChanges, change)
			}
			headCursor, err = tx.CurrentCursor(ctx)
			if err != nil {
				return err
			}
			records, err := tx.ListRecords(ctx)
			if err != nil {
				return err
			}
			progressDay, err := deriveProgressDay(records)
			if err != nil {
				return err
			}
			version := uint64(0)
			if len(snapshot) > 0 {
				version = 1
			}
			lineage = Lineage{
				BaselineID:  resultBaselineID,
				Version:     version,
				UpdatedAtMs: now,
				ProgressDay: progressDay,
			}
			if err := tx.SetLineage(ctx, lineage); err != nil {
				return err
			}
			receipt = newResolutionReceipt(request, lineage, headCursor)
			if err := tx.InsertResolutionReceipt(ctx, receipt); err != nil {
				return err
			}
			for _, change := range archiveChanges {
				if err := tx.ArchiveChange(ctx, lineage.BaselineID, lineage.Version, now, change); err != nil {
					return err
				}
			}
			if err := tx.UpsertArchiveHead(ctx, lineage.BaselineID, headCursor, lineage.Version, now); err != nil {
				return err
			}
			if err := tx.InsertRealtimeEvent(ctx, RealtimeEvent{
				Type:               "sync_hint",
				BaselineID:         lineage.BaselineID,
				ServerCursor:       headCursor,
				ServerVersion:      lineage.Version,
				OriginConnectionID: metadata.OriginConnectionID,
				CreatedAtMs:        now,
			}); err != nil {
				return err
			}
			result = resolveResult(request.GetDeviceId(), lineage, headCursor, records, false, true)
			return nil
		}

		receipt = newResolutionReceipt(request, lineage, headCursor)
		if err := tx.InsertResolutionReceipt(ctx, receipt); err != nil {
			return err
		}
		records, err := tx.ListRecords(ctx)
		if err != nil {
			return err
		}
		result = resolveResult(request.GetDeviceId(), lineage, headCursor, records, false, false)
		return nil
	})
	return result, err
}

type mutationBatch struct {
	acks             []*syncv1.MutationAck
	canonicalChanges []*syncv1.Change
	forcedChanges    []*syncv1.Change
	archiveChanges   []*syncv1.Change
	appliedCount     int
}

func applyMutations(
	ctx context.Context,
	tx Tx,
	deviceID string,
	mutations []*syncv1.Mutation,
	now uint64,
) (mutationBatch, error) {
	batch := mutationBatch{
		acks:             make([]*syncv1.MutationAck, 0, len(mutations)),
		canonicalChanges: make([]*syncv1.Change, 0, len(mutations)),
		forcedChanges:    make([]*syncv1.Change, 0),
		archiveChanges:   make([]*syncv1.Change, 0),
	}
	for _, mutation := range mutations {
		if err := validateMutation(mutation, deviceID); err != nil {
			return mutationBatch{}, err
		}
		receipt, err := tx.GetReceipt(ctx, mutation.GetOpId())
		if err == nil {
			canonical, err := tx.GetOperation(ctx, receipt.ServerCursor)
			if err != nil {
				return mutationBatch{}, err
			}
			batch.acks = append(batch.acks, receiptAck(receipt))
			batch.canonicalChanges = append(batch.canonicalChanges, canonical)
			continue
		}
		if !errors.Is(err, ErrNotFound) {
			return mutationBatch{}, err
		}

		existing, err := tx.GetRecord(ctx, mutation.GetEntityKey())
		if err != nil && !errors.Is(err, ErrNotFound) {
			return mutationBatch{}, err
		}
		conflict := existing != nil && existing.GetCursor() > mutation.GetBaseVersion()
		applied := existing == nil || existing.GetCursor() <= mutation.GetBaseVersion()
		serverCursor := uint64(0)
		if existing != nil {
			serverCursor = existing.GetCursor()
		}
		var canonical *syncv1.Change
		if applied {
			serverCursor, err = tx.NextCursor(ctx)
			if err != nil {
				return mutationBatch{}, err
			}
			canonical = mutationChange(mutation, serverCursor, clampClientTime(mutation.GetClientTimeMs(), now))
			if err := tx.AppendOperation(ctx, canonical); err != nil {
				return mutationBatch{}, err
			}
			if err := tx.UpsertRecord(ctx, canonical); err != nil {
				return mutationBatch{}, err
			}
			batch.archiveChanges = append(batch.archiveChanges, canonical)
			batch.appliedCount++
		} else {
			canonical = existing
			batch.forcedChanges = append(batch.forcedChanges, existing)
		}
		receipt = Receipt{
			OpID:         mutation.GetOpId(),
			ServerCursor: serverCursor,
			Conflict:     conflict,
			Applied:      applied,
		}
		if err := tx.InsertReceipt(ctx, receipt); err != nil {
			return mutationBatch{}, err
		}
		batch.acks = append(batch.acks, receiptAck(receipt))
		batch.canonicalChanges = append(batch.canonicalChanges, canonical)
	}
	return batch, nil
}

func finalizeMutationBatch(
	ctx context.Context,
	tx Tx,
	metadata CommandMetadata,
	lineage Lineage,
	batch mutationBatch,
	now uint64,
) (Lineage, uint64, error) {
	var err error
	if batch.appliedCount > 0 {
		records, err := tx.ListRecords(ctx)
		if err != nil {
			return Lineage{}, 0, err
		}
		progressDay, err := deriveProgressDay(records)
		if err != nil {
			return Lineage{}, 0, err
		}
		lineage, err = tx.IncrementLineage(ctx, now, progressDay)
		if err != nil {
			return Lineage{}, 0, err
		}
	}
	headCursor, err := tx.CurrentCursor(ctx)
	if err != nil {
		return Lineage{}, 0, err
	}
	if batch.appliedCount == 0 {
		return lineage, headCursor, nil
	}
	for _, change := range batch.archiveChanges {
		if err := tx.ArchiveChange(ctx, lineage.BaselineID, lineage.Version, now, change); err != nil {
			return Lineage{}, 0, err
		}
	}
	if err := tx.UpsertArchiveHead(ctx, lineage.BaselineID, headCursor, lineage.Version, now); err != nil {
		return Lineage{}, 0, err
	}
	if err := tx.InsertRealtimeEvent(ctx, RealtimeEvent{
		Type:               "sync_hint",
		BaselineID:         lineage.BaselineID,
		ServerCursor:       headCursor,
		ServerVersion:      lineage.Version,
		OriginConnectionID: metadata.OriginConnectionID,
		CreatedAtMs:        now,
	}); err != nil {
		return Lineage{}, 0, err
	}
	return lineage, headCursor, nil
}

func validateDiffResponseSize(response *syncv1.DiffResponse) error {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(response)
	if err != nil {
		return err
	}
	if len(encoded) > maxDiffResponseBytes {
		return ErrResponseTooLarge
	}
	return nil
}

func normalizedFeedLimit(value int) int {
	if value == 0 {
		return int(defaultPullLimit)
	}
	if value > int(maxPullLimit) {
		return int(maxPullLimit)
	}
	return value
}

func (s *Service) ensureLineage(ctx context.Context, tx Tx, requestedBaselineID string, now uint64) (Lineage, error) {
	lineage, err := tx.GetLineage(ctx)
	if err == nil {
		return lineage, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Lineage{}, err
	}
	cursor, err := tx.CurrentCursor(ctx)
	if err != nil {
		return Lineage{}, err
	}
	lineage = Lineage{BaselineID: requestedBaselineID, ProgressDay: campaignStart}
	if cursor > 0 {
		lineage.BaselineID, err = s.newBaselineIDFunc()
		if err != nil {
			return Lineage{}, err
		}
		lineage.UpdatedAtMs = now
		records, err := tx.ListRecords(ctx)
		if err != nil {
			return Lineage{}, err
		}
		lineage.ProgressDay, err = deriveProgressDay(records)
		if err != nil {
			return Lineage{}, err
		}
	}
	if err := tx.InsertLineage(ctx, lineage); err != nil {
		return Lineage{}, err
	}
	return lineage, nil
}

func mutationChange(mutation *syncv1.Mutation, cursor, clientTimeMs uint64) *syncv1.Change {
	return &syncv1.Change{
		Cursor:       cursor,
		EntityKey:    mutation.GetEntityKey(),
		ValueJson:    append([]byte(nil), mutation.GetValueJson()...),
		Deleted:      mutation.GetDeleted(),
		DeviceId:     mutation.GetDeviceId(),
		ClientTimeMs: clientTimeMs,
		OpId:         mutation.GetOpId(),
	}
}

func clampClientTime(value, now uint64) uint64 {
	if value < 1 {
		return 1
	}
	if value > now+maxFutureSkewMs {
		return now + maxFutureSkewMs
	}
	return value
}

func receiptAck(receipt Receipt) *syncv1.MutationAck {
	return &syncv1.MutationAck{
		OpId:         receipt.OpID,
		ServerCursor: receipt.ServerCursor,
		Conflict:     receipt.Conflict,
		Applied:      receipt.Applied,
	}
}

func changeIdentity(change *syncv1.Change) string {
	return fmt.Sprintf("%d:%s", change.GetCursor(), change.GetEntityKey())
}

func sameResolutionRequest(receipt ResolutionReceipt, request *syncv1.ResolveBaselineRequest) bool {
	return receipt.LocalBaselineID == request.GetLocalBaselineId() &&
		receipt.ExpectedServerBaselineID == request.GetExpectedServerBaselineId() &&
		receipt.ExpectedServerVersion == request.GetExpectedServerVersion() &&
		receipt.Choice == request.GetChoice()
}

func newResolutionReceipt(request *syncv1.ResolveBaselineRequest, lineage Lineage, cursor uint64) ResolutionReceipt {
	return ResolutionReceipt{
		RequestID:                request.GetRequestId(),
		LocalBaselineID:          request.GetLocalBaselineId(),
		ExpectedServerBaselineID: request.GetExpectedServerBaselineId(),
		ExpectedServerVersion:    request.GetExpectedServerVersion(),
		Choice:                   request.GetChoice(),
		ResultBaselineID:         lineage.BaselineID,
		ResultCursor:             cursor,
		ResultVersion:            lineage.Version,
		ResultUpdatedAtMs:        lineage.UpdatedAtMs,
	}
}

func resolveResult(
	deviceID string,
	lineage Lineage,
	cursor uint64,
	records []*syncv1.Change,
	stale bool,
	stateChanged bool,
) *ResolveResult {
	return &ResolveResult{
		Response: &syncv1.ResolveBaselineResponse{
			BaselineId:        lineage.BaselineID,
			ServerVersion:     lineage.Version,
			ServerUpdatedAtMs: lineage.UpdatedAtMs,
			ServerProgressDay: lineage.ProgressDay,
			Records:           records,
			Stale:             stale,
			ServerCursor:      cursor,
		},
		StateChanged:  stateChanged,
		DeviceID:      deviceID,
		BaselineID:    lineage.BaselineID,
		ServerCursor:  cursor,
		ServerVersion: lineage.Version,
	}
}

func randomBaselineID() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return "baseline_" + hex.EncodeToString(random), nil
}

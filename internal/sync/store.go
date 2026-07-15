package sync

import (
	"context"
	"errors"

	syncv1 "github.com/Luminet2023/hifumi-backend/api/sync/v1"
)

var ErrNotFound = errors.New("sync row not found")

// Store 在 owner 粒度开启事务。实现必须先锁定 owner 协调行，再执行 fn。
// 该锁是 Durable Object 单用户串行语义在 MySQL 中的替代物。
type Store interface {
	WithOwnerTransaction(ctx context.Context, ownerKey string, fn func(Tx) error) error
	// ReadFeedSnapshot 在短只读事务的一致性快照中读取当前 lineage/head
	// 以及 afterCursor 之后的操作。实现不得获取 owner 写锁。
	ReadFeedSnapshot(ctx context.Context, ownerKey string, afterCursor uint64, limit int) (FeedSnapshot, error)
}

type Tx interface {
	CurrentCursor(ctx context.Context) (uint64, error)
	NextCursor(ctx context.Context) (uint64, error)

	GetLineage(ctx context.Context) (Lineage, error)
	InsertLineage(ctx context.Context, lineage Lineage) error
	SetLineage(ctx context.Context, lineage Lineage) error
	IncrementLineage(ctx context.Context, updatedAtMs uint64, progressDay string) (Lineage, error)

	GetRecord(ctx context.Context, entityKey string) (*syncv1.Change, error)
	ListRecords(ctx context.Context) ([]*syncv1.Change, error)
	GetOperation(ctx context.Context, cursor uint64) (*syncv1.Change, error)
	PullOperations(ctx context.Context, afterCursor uint64, limit int) ([]*syncv1.Change, error)
	AppendOperation(ctx context.Context, change *syncv1.Change) error
	UpsertRecord(ctx context.Context, change *syncv1.Change) error

	GetReceipt(ctx context.Context, opID string) (Receipt, error)
	InsertReceipt(ctx context.Context, receipt Receipt) error

	GetResolutionReceipt(ctx context.Context, requestID string) (ResolutionReceipt, error)
	InsertResolutionReceipt(ctx context.Context, receipt ResolutionReceipt) error
	ClearCurrentBaseline(ctx context.Context) error

	GetArchiveHead(ctx context.Context, baselineID string) (ArchiveHead, error)
	ArchiveChange(ctx context.Context, baselineID string, serverVersion uint64, archivedAtMs uint64, change *syncv1.Change) error
	UpsertArchiveHead(ctx context.Context, baselineID string, cursor uint64, serverVersion uint64, updatedAtMs uint64) error
	InsertRealtimeEvent(ctx context.Context, event RealtimeEvent) error
}

type Lineage struct {
	BaselineID  string
	Version     uint64
	UpdatedAtMs uint64
	ProgressDay string
}

type FeedSnapshot struct {
	Lineage    Lineage
	HeadCursor uint64
	Changes    []*syncv1.Change
}

type Receipt struct {
	OpID         string
	ServerCursor uint64
	Conflict     bool
	Applied      bool
}

type ResolutionReceipt struct {
	RequestID                string
	LocalBaselineID          string
	ExpectedServerBaselineID string
	ExpectedServerVersion    uint64
	Choice                   syncv1.BaselineChoice
	ResultBaselineID         string
	ResultCursor             uint64
	ResultVersion            uint64
	ResultUpdatedAtMs        uint64
}

type ArchiveHead struct {
	Cursor      uint64
	Version     uint64
	UpdatedAtMs uint64
}

type RealtimeEvent struct {
	Type               string
	BaselineID         string
	ServerCursor       uint64
	ServerVersion      uint64
	OriginConnectionID string
	CreatedAtMs        uint64
}

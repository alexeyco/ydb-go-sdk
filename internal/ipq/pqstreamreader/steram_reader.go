package pqstreamreader

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
)

type Codec int

type PartitionSessionID int64
type Offset int64

type StreamReader struct {
	g Ydb_PersQueue_V1.PersQueueService_MigrationStreamingReadClient

	OnStartPartitionSessionRequest func(ctx context.Context, req StartPartitionSessionRequest) (StartPartitionSessionResponse, error)
	OnStopPartitionSessionRequest  func(ctx context.Context, req StopPartitionSessionRequest) (StopPartitionSessionResponse, error)
}

func NewStreamReader() *StreamReader {
	panic("not implemented")
}

//
// OnStartPartitionSessionRequest
//

type StartPartitionSessionRequest struct {
	PartitionSession PartitionSession
	CommittedOffset  Offset
	EndOffset        Offset
}
type PartitionSession struct {
	Topic              string
	PartitionID        int64
	PartitionSessionID PartitionSessionID
}
type StartPartitionSessionResponse struct {
	PartitionSessionID PartitionSessionID
	ReadOffset         Offset
	ReadOffsetUse      bool // ReadOffset used only if ReadOffsetUse=true to distinguish zero and unset variables
	CommitOffset       Offset
	CommitOffsetUse    bool // CommitOffset used only if CommitOffsetUse=true to distinguish zero and unset variables
}

//
// OnStopPartitionSessionRequest
//

type StopPartitionSessionRequest struct {
	PartitionSessionID PartitionSessionID
	Graceful           bool
	CommittedOffset    Offset
}
type StopPartitionSessionResponse struct {
	PartitionSessionID PartitionSessionID
}

//
// InitRequest
//

type TopicReadSettings struct {
	// Topic path.
	Topic string

	// Partition groups that will be read by this session.
	// If list is empty - then session will read all partition groups.
	PartitionGroupsID []int64

	// Read data only after this timestamp from this topic.
	StartFromWrittenAtMs int64
}

type PartitionSessionState struct {
	Status int // TODO: Enum from pb
}

type State struct {
}

type InitRequest struct {
	// Message that describes topic to read.
	// Topics that will be read by this session.
	TopicsReadSettings []TopicReadSettings

	// Flag that indicates reading only of original topics in cluster or all including mirrored.
	ReadOnlyOriginal bool

	// Path of consumer that is used for reading by this session.
	Consumer string

	// Skip all messages that has write timestamp smaller than now - max_time_lag_ms.
	MaxLagDurationMs int64

	// Read data only after this timestamp from all topics.
	StartFromWrittenAtMs int64

	// Maximum block format version supported by the client. Server will asses this parameter and return actual data blocks version in
	// StreamingReadServerMessage.InitResponse.block_format_version_by_topic (and StreamingReadServerMessage.AddTopicResponse.block_format_version)
	// or error if client will not be able to read data.
	MaxSupportedFormatVersion int64

	// Maximal size of client cache for message_group_id, ip and meta, per partition.
	// There are separate caches for each partition partition sessions.
	// There are separate caches for message group identifiers, ip and meta inside one partition session.
	MaxMetaCacheSize int64

	// Session identifier for retries. Must be the same as session_id from Inited server response. If this is first connect, not retry - do not use this field.
	SessionID string

	// 0 for first init message and incremental value for connect retries.
	ConnectionAttempt int64

	// Formed state for retries. If not retry - do not use this field.
	State State

	IdleTimeoutMs int64
}

type InitResponse struct {
	SessionID string
}

func (r *StreamReader) InitRequest(ctx context.Context, req InitRequest) (InitResponse, error) {
	panic("not implemented")
}

type ReadRequest struct {
	BytesSize int64
}

type ReadResponse struct {
	Partitions []PartitionData
}
type PartitionData struct {
	PartitionSessionID int64

	Batches []Batch
}
type Batch struct {
	MessageGroupID string
	SessionMeta    map[string]string
	WriteTimeStamp time.Time
	WriterIP       string

	Messages []MessageData
}

type MessageData struct {
	Offset int64
	SeqNo  int64

	// Timestamp of creation of message provided on write from client.
	Created          time.Time
	Codec            Codec
	Data             []byte
	UncompressedSize int64

	// Filled if partition_key / hash was used on message write.
	PartitionKey string
	ExplicitHash []byte
}

func (r *StreamReader) ReadRequest(ctx context.Context, req ReadRequest) (ReadResponse, error) {
	panic("not implemented")
}

//
// CommitOffsetRequest
//

type CommitOffsetRequest struct {
	Partitions []PartitionCommitOffset
}
type PartitionCommitOffset struct {
	PartitionSessionID PartitionSessionID
	Offsets            []OffsetRange
}

// OffsetRange represents range [start_offset, end_offset).
type OffsetRange struct {
	Start Offset
	End   Offset
}

type CommitOffsetResponse struct {
	Committed []PartitionCommittedOffset
}
type PartitionCommittedOffset struct {
	PartitionSessionID PartitionSessionID
	Committed          Offset
}

func (r *StreamReader) CommitOffsetRequest(ctx context.Context, req CommitOffsetRequest) (CommitOffsetResponse, error) {
	panic("not implemented")
}

//
// PartitionSessionStatusRequest
//

type PartitionSessionStatusRequest struct {
	PartitionSessionID PartitionSessionID
}
type PartitionSessionStatusResponse struct {
	PartitionSessionID PartitionSessionID
	Committed          Offset
	End                Offset
	WrittenAtWatermark time.Time
}

func (r *StreamReader) PartitionSessionStatusRequest(ctx context.Context, req PartitionSessionStatusRequest) (PartitionSessionStatusResponse, error) {
	panic("not implemented")
}

//
// UpdateTokenRequest
//

type UpdateTokenRequest struct {
	Token string
}
type UpdateTokenResponse struct{}

func (r *StreamReader) UpdateTokenRequest(ctx context.Context, req UpdateTokenRequest) (UpdateTokenResponse, error) {
	panic("not implemented")
}

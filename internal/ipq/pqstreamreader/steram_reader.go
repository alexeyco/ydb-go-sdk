package pqstreamreader

import (
	"time"

	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
)

type Codec int

type PartitionSessionID int64
type Offset int64

type StatusCode int

type StreamReader struct {
	Stream GrpcStream
}

type GrpcStream interface {
	Send(messageNew *Ydb_PersQueue_V12.StreamingReadClientMessageNew) error
	Recv() (*Ydb_PersQueue_V12.StreamingReadServerMessageNew, error)
}

func (s StreamReader) Recv() (ServerStreamMessage, error) {
	panic("not implemented")
}

func (s StreamReader) Send(mess ClientStreamMessage) error {
	panic("not implemented")
}

//
// ClientStreamMessage
//

type ClientStreamMessage struct {
	ClientMessage clientMessage
}

type clientMessage interface {
	isClientMessage()
}

type clientMessageImpl struct{}

func (clientMessageImpl) isClientMessage() {}

//
// ServerStreamMessage
//

type ServerStreamMessage struct {
	Status StatusCode
	Issues []YdbIssueMessage

	ServerMessage serverMessage
}

type YdbIssueMessage struct {
}

type serverMessage interface {
	isServerMessage()
}
type serverMessageImpl struct{}

func (serverMessageImpl) isServerMessage() {}

//
// StartPartitionSessionRequest
//

type StartPartitionSessionRequest struct {
	serverMessageImpl

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
	clientMessageImpl

	PartitionSessionID PartitionSessionID
	ReadOffset         Offset
	ReadOffsetUse      bool // ReadOffset used only if ReadOffsetUse=true to distinguish zero and unset variables
	CommitOffset       Offset
	CommitOffsetUse    bool // CommitOffset used only if CommitOffsetUse=true to distinguish zero and unset variables
}

//
// StopPartitionSessionRequest
//

type StopPartitionSessionRequest struct {
	serverMessageImpl

	PartitionSessionID PartitionSessionID
	Graceful           bool
	CommittedOffset    Offset
}
type StopPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
}

//
// InitRequest
//

type InitRequest struct {
	clientMessageImpl

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

type InitResponse struct {
	serverMessageImpl

	SessionID string
}

//
// ReadRequest
//

type ReadRequest struct {
	clientMessageImpl

	BytesSize int64
}

type ReadResponse struct {
	serverMessageImpl

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

//
// CommitOffsetRequest
//

type CommitOffsetRequest struct {
	clientMessageImpl

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
	serverMessageImpl

	Committed []PartitionCommittedOffset
}
type PartitionCommittedOffset struct {
	PartitionSessionID PartitionSessionID
	Committed          Offset
}

//
// PartitionSessionStatusRequest
//

type PartitionSessionStatusRequest struct {
	clientMessageImpl
	PartitionSessionID PartitionSessionID
}
type PartitionSessionStatusResponse struct {
	serverMessageImpl

	PartitionSessionID PartitionSessionID
	Committed          Offset
	End                Offset
	WrittenAtWatermark time.Time
}

//
// UpdateTokenRequest
//

type UpdateTokenRequest struct {
	clientMessageImpl

	Token string
}
type UpdateTokenResponse struct {
	serverMessageImpl
}

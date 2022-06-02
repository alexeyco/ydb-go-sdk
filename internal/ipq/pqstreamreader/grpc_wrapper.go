package pqstreamreader

import (
	"time"

	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
)

type Codec int

const (
	CodecAuto Codec = -1
)

const (
	CodecUnknown Codec = iota
	CodecRaw
)

type PartitionSessionID int64
type Offset int64

type StatusCode int

const StatusOk StatusCode = 1 // TODO: <-- actualize statuses

type StreamReader struct {
	Stream GrpcStream
}

type GrpcStream interface {
	Send(messageNew *Ydb_PersQueue_V12.StreamingReadClientMessageNew) error
	Recv() (*Ydb_PersQueue_V12.StreamingReadServerMessageNew, error)
}

func (s StreamReader) Close() error {
	panic("not implemented")
}

func (s StreamReader) Recv() (ServerMessage, error) {
	grpcMess, err := s.Stream.Recv()
	if err != nil {
		return nil, err
	}

	var meta ServerMessageMetadata

	switch m := grpcMess.ServerMessage.(type) {
	case *Ydb_PersQueue_V12.StreamingReadServerMessageNew_BatchReadResponse_:
		resp := &ReadResponse{}
		resp.ServerMessageMetadata = meta
		resp.Partitions = make([]PartitionData, 0, len(m.BatchReadResponse.Partitions))
		panic("not implemented")
		return resp, nil
	}

	panic("not implemented")
}

func (s StreamReader) Send(mess ClientMessage) error {
	switch m := mess.(type) {
	case *CommitOffsetRequest:
		grpcMess := &Ydb_PersQueue_V12.StreamingReadClientMessageNew{}
		grpcMess.ClientMessage = &Ydb_PersQueue_V12.StreamingReadClientMessageNew_CommitRequest_{
			CommitRequest: &Ydb_PersQueue_V12.StreamingReadClientMessageNew_CommitRequest{Commits: make([]*Ydb_PersQueue_V12.StreamingReadClientMessageNew_PartitionCommit, 0, len(m.Partitions))},
		}
		_ = grpcMess
	}

	panic("not implemented")
}

//
// ClientStreamMessage
//

type ClientMessage interface {
	isClientMessage()
}

type clientMessageImpl struct{}

func (*clientMessageImpl) isClientMessage() {}

type ServerMessageMetadata struct {
	Status StatusCode
	Issues []YdbIssueMessage
}

func (s ServerMessageMetadata) StatusData() ServerMessageMetadata {
	return s
}

type YdbIssueMessage struct {
}

type ServerMessage interface {
	isServerMessage()
	StatusData() ServerMessageMetadata
}

type serverMessageImpl struct {
}

func (*serverMessageImpl) isServerMessage() {}

//
// StartPartitionSessionRequest
//

type StartPartitionSessionRequest struct {
	serverMessageImpl

	ServerMessageMetadata
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

	ServerMessageMetadata
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

	// Path of consumer that is used for reading by this session.
	Consumer string

	// Skip all messages that has write timestamp smaller than now - max_time_lag_ms.
	MaxLagDuration time.Duration

	// Read data only after this timestamp from all topics.
	StartFromWrittenAt time.Time

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
	StartFromWritten time.Time
}

type PartitionSessionState struct {
	Status int // TODO: Enum from pb
}

type State struct {
}

type InitResponse struct {
	serverMessageImpl

	ServerMessageMetadata
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

	ServerMessageMetadata
	Partitions []PartitionData
}
type PartitionData struct {
	PartitionSessionID PartitionSessionID

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
	Offset Offset
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

	ServerMessageMetadata
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

	ServerMessageMetadata
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

	ServerMessageMetadata
}

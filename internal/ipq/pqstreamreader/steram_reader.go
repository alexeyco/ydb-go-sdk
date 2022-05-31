package pqstreamreader

import "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"

type StreamReader struct {
	g Ydb_PersQueue_V1.PersQueueService_MigrationStreamingReadClient
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

func (r *StreamReader) InitRequest(req InitRequest) error {

}

package ipq

import (
	"fmt"

	pqproto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/pq"
)

type streamOptions struct {
	readRules    []*pqproto.TopicSettings_ReadRule
	remoteMirror *pqproto.TopicSettings_RemoteMirrorRule
}

func (o *streamOptions) AddReadRule(r pq.ReadRule) {
	o.readRules = append(o.readRules, encodeReadRule(r))
}

func (o *streamOptions) SetRemoteMirrorRule(r pq.RemoteMirrorRule) {
	o.remoteMirror = encodeRemoteMirrorRule(r)
}

func encodeTopicSettings(settings pq.StreamSettings, opts ...pq.StreamOption) *pqproto.TopicSettings {
	optValues := streamOptions{}
	for _, o := range opts {
		o(&optValues)
	}

	// TODO: fix
	return &pqproto.TopicSettings{
		PartitionsCount: int32(settings.PartitionsCount),
		//RetentionPeriodMs:                    settings.RetentionPeriod.Milliseconds(),
		MessageGroupSeqnoRetentionPeriodMs:   settings.MessageGroupSeqnoRetentionPeriod.Milliseconds(),
		MaxPartitionMessageGroupsSeqnoStored: int64(settings.MaxPartitionMessageGroupsSeqnoStored),
		SupportedFormat:                      encodeFormat(settings.SupportedFormat),
		SupportedCodecs:                      encodeCodecs(settings.SupportedCodecs),
		MaxPartitionStorageSize:              int64(settings.MaxPartitionStorageSize),
		MaxPartitionWriteSpeed:               int64(settings.MaxPartitionWriteSpeed),
		MaxPartitionWriteBurst:               int64(settings.MaxPartitionWriteBurst),
		ClientWriteDisabled:                  settings.ClientWriteDisabled,
		ReadRules:                            optValues.readRules,
		RemoteMirrorRule:                     optValues.remoteMirror,
	}
}

func encodeReadRule(r pq.ReadRule) *pqproto.TopicSettings_ReadRule {
	return &pqproto.TopicSettings_ReadRule{
		ConsumerName:               string(r.Consumer),
		Important:                  r.Important,
		StartingMessageTimestampMs: r.StartingMessageTimestamp.UnixMilli(),
		SupportedFormat:            encodeFormat(r.SupportedFormat),
		SupportedCodecs:            encodeCodecs(r.Codecs),
		Version:                    int64(r.Version),
		ServiceType:                r.ServiceType,
	}
}

func encodeRemoteMirrorRule(r pq.RemoteMirrorRule) *pqproto.TopicSettings_RemoteMirrorRule {
	return &pqproto.TopicSettings_RemoteMirrorRule{
		Endpoint:                   r.Endpoint,
		TopicPath:                  string(r.SourceStream),
		ConsumerName:               string(r.Consumer),
		Credentials:                encodeRemoteMirrorCredentials(r.Credentials),
		StartingMessageTimestampMs: r.StartingMessageTimestamp.UnixMilli(),
		Database:                   r.Database,
	}
}

func encodeCodecs(v []pq.Codec) []pqproto.Codec {
	result := make([]pqproto.Codec, len(v))
	for i := range result {
		switch v[i] {
		case pq.CodecUnspecified:
			result[i] = pqproto.Codec_CODEC_UNSPECIFIED
		case pq.CodecRaw:
			result[i] = pqproto.Codec_CODEC_RAW
		case pq.CodecGzip:
			result[i] = pqproto.Codec_CODEC_GZIP
		case pq.CodecLzop:
			result[i] = pqproto.Codec_CODEC_LZOP
		case pq.CodecZstd:
			result[i] = pqproto.Codec_CODEC_ZSTD
		default:
			panic(fmt.Sprintf("unknown codec value %v", v))
		}
	}
	return result
}

func encodeFormat(v pq.Format) pqproto.TopicSettings_Format {
	switch v {
	case pq.FormatUnspecified:
		return pqproto.TopicSettings_FORMAT_UNSPECIFIED
	case pq.FormatBase:
		return pqproto.TopicSettings_FORMAT_BASE
	default:
		panic(fmt.Sprintf("unknown format value %v", v))
	}
}

func encodeRemoteMirrorCredentials(v pq.RemoteMirrorCredentials) *pqproto.Credentials {
	if v == nil {
		return nil
	}
	switch c := v.(type) {
	case pq.IAMCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_Iam_{
				Iam: &pqproto.Credentials_Iam{
					Endpoint:          c.Endpoint,
					ServiceAccountKey: c.ServiceAccountKey,
				},
			},
		}
	case pq.JWTCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_JwtParams{
				JwtParams: string(c),
			},
		}
	case pq.OAuthTokenCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_OauthToken{
				OauthToken: string(c),
			},
		}
	}
	panic(fmt.Sprintf("unknown credentials type %T", v))
}

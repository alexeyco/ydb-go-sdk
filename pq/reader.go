package pq

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
)

type ReaderStream interface {
	Recv() (pqstreamreader.ServerMessage, error)
	Send(mess pqstreamreader.ClientMessage) error
	Close() error
}

type ReaderStreamConnector interface {
	Connect(ctx context.Context) (ReaderStream, error)
}

type Reader struct {
	connector ReaderStreamConnector

	m         sync.RWMutex
	streamVal ReaderStream
}

func NewReader(consumer string, readSelectors []ReadSelector) *Reader {
	panic("not implemented")
}

func (r *Reader) stream() ReaderStream {
	r.m.RLock()
	defer r.m.RUnlock()

	return r.streamVal
}

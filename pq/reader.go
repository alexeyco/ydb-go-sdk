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

func (r *Reader) ReadMessageBatch(context.Context, ...ReadBatchOption) (Batch, error) {
	return Batch{}, nil
}

func (r *Reader) ReadMessage(context.Context) (Message, error) {
	return Message{}, nil
}

func (r *Reader) Commit(ctx context.Context, commits ...CommitableByOffset) error {
	batch := make(CommitBatch, 0, len(commits))
	batch.Append(commits...)
	return r.CommitBatch(ctx, batch)
}

func (r *Reader) CommitBatch(ctx context.Context, batch CommitBatch) error {
	panic("not implemented")
}

type PartitionController struct{}

func (pc *PartitionController) OnSessionStart(callback func(info *pqstreamreader.StartPartitionSessionRequest, response *pqstreamreader.StartPartitionSessionResponse) error) {
	/*
		callback будет вызываться при каждом старте партиций
		info - информация о новой партиции
		response - предполагаемый ответ серверу, предзаполненный SDK, который можно менять.
	*/
}

func (pc *PartitionController) OnSessionShutdown(callback func(info *pqstreamreader.StopPartitionSessionRequest, response *pqstreamreader.StopPartitionSessionResponse) error) {
	/*
		callback будет вызываться при каждом стопе
		info - информация о новой партиции
		response - предполагаемый ответ серверу, предзаполненный SDK, который можно менять.
	*/
}

func (pc *PartitionController) OnPartitionStatus(callback func(info *pqstreamreader.PartitionSessionStatusResponse)) {

}

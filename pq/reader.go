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
	streamVal *readerPump
}

func NewReader(consumer string, readSelectors []ReadSelector) *Reader {
	panic("not implemented")
}

func (r *Reader) stream() *readerPump {
	r.m.RLock()
	defer r.m.RUnlock()

	return r.streamVal
}

func (r *Reader) ReadMessageBatch(context.Context, ...ReadBatchOption) (Batch, error) {
	panic("not implemented")
}

func (r *Reader) ReadMessage(context.Context) (Message, error) {
	return Message{}, nil
}

func (r *Reader) CommitMessage(ctx context.Context, messages ...Message) error {
	batch := make(CommitBatch, 0, len(messages))
	batch.AppendMessages(messages...)
	return r.stream().Commit(ctx, batch)
}

func (r *Reader) CommitBatch(ctx context.Context, batch Batch) error {
	panic("not implemented")
}

func (r *Reader) PartitionControler() *PartitionController {
	panic("not implemented")
}

type PartitionController struct{}

// TODO: объявить более специализированные типы

type StartPartitionSessionRequest struct {
	Session         *PartitionSession
	CommittedOffset int64
	EndOffset       int64
}

type StartPartitionSessionResponse = pqstreamreader.StartPartitionSessionResponse
type StopPartitionSessionRequest = pqstreamreader.StopPartitionSessionRequest
type StopPartitionSessionResponse = pqstreamreader.StopPartitionSessionResponse
type PartitionSessionStatusResponse = pqstreamreader.PartitionSessionStatusResponse

func (pc *PartitionController) OnSessionStart(callback func(info *StartPartitionSessionRequest, response *StartPartitionSessionResponse) error) {
	/*
		callback будет вызываться при каждом старте партиций
		info - информация о новой партиции
		response - предполагаемый ответ серверу, предзаполненный SDK, который можно менять.
	*/
}

func (pc *PartitionController) OnSessionShutdown(callback func(info *StopPartitionSessionRequest, response *StopPartitionSessionResponse) error) {
	/*
		callback будет вызываться при каждом стопе
		info - информация о новой партиции
		response - предполагаемый ответ серверу, предзаполненный SDK, который можно менять.
	*/
}

func (pc *PartitionController) OnPartitionStatus(callback func(info *PartitionSessionStatusResponse)) {

}

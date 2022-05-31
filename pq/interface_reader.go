package pq

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

type Reader struct {
	stream *ReadStream
}

type ReaderConfig struct {
	PartitionStreamDestroyTimeout time.Duration // rekby: зачем?

	UserHandlers struct {
		CreatePartitionStream  StartReadingValidator
		DestroyPartitionStream StopReadingHandler
	}
	// и прочие полезные опции вроде размера inflight ...

}

type readerOption func()

func WithMaxMemoryUsageBytes(size int) readerOption {
	panic("not implemented")
}

func WithRetry(backoff backoff.Backoff) readerOption {
	panic("not implemented")
}

func WithSyncCommit(enabled bool) readerOption {
	panic("not implemented")
}

func WithReadSelector(readSelector ReadSelector) readerOption {
	panic("not implemented")
}

func NewReader(consumer string, readSelectors []ReadSelector) (*Reader, error) {
	return nil, nil
}

func (r *Reader) Close() error {
	// check alive
	// stop send
	// stop read
	// cancel all allocations -> cancel all batches and messages
	return nil
}

func (r *Reader) CloseWithContext(ctx context.Context) {
	panic("not implemented")
}

// ReadBatchOption для различных пожеланий к батчу вроде WithMaxMessages(int)
type ReadBatchOption func()

func (r *Reader) ReadMessageBatch(context.Context, ...ReadBatchOption) (Batch, error) {
	return Batch{}, nil
}

func (r *Reader) ReadMessage(context.Context) (Message, error) {
	return Message{}, nil
}

func (r *Reader) Commit(context.Context, ...CommitableByOffset) error {
	// Note: в пределах assign диапазоны оффсетов сообщений собираются на сервере.
	// Т.е. фактический коммит сообщеинй произойдет когда закоммитятся все предыдущие сообщения.
	// Это значит что тут может быть минимум логики
	return nil
}

func (r *Reader) CommitBatch(ctx context.Context, batch CommitBatch) error {
	return r.Commit(ctx, batch...)
}

func DeferMessageCommits(msg ...Message) []CommitableByOffset {
	// кажется тут можно сразу собрать интервалы оффсетов
	result := make([]CommitableByOffset, len(msg))
	for i := range msg {
		result[i] = msg[i].CommitOffset
	}
	return result
}

func (r *Reader) Stats() ReaderStats {
	// Нужна настройка и периодические запросы PartitionStreamState
	// Возвращать из памяти
	return ReaderStats{}
}

func (r *Reader) PartitionStreamState(context.Context, PartitionStream) (PartitionStreamState, error) {
	// метод для запроса статуса конкретного стрима с сервера, синхронный
	return PartitionStreamState{}, nil
}

type ReaderStats struct {
	PartitionStreams []PartitionStream
	// other internal stats
}

type StartReadingValidator interface {
	ValidateReadStart(context.Context, *CreatePartitionStreamResponse) error
}

type StopReadingHandler interface {
	OnReadStop(context.Context, DestroyPartitionStreamRequest)
}

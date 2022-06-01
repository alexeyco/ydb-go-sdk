package pq

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type readerPump struct {
	ctx    context.Context
	cancel context.CancelFunc

	freeBytes chan int64
	stream    ReaderStream

	setErr sync.Once
	err    error

	readResponsesParseSignal chan struct{}
	messageBatches           chan Batch

	m             sync.Mutex
	readResponses []*pqstreamreader.ReadResponse // use slice instead channel for gurantee receive data without block
}

func newReaderPump(ctx context.Context, bufferSize int64, stream ReaderStream) *readerPump {
	ctx, cancel := context.WithCancel(ctx)
	res := &readerPump{
		ctx:                      ctx,
		freeBytes:                make(chan int64, 1),
		stream:                   stream,
		cancel:                   cancel,
		readResponsesParseSignal: make(chan struct{}, 1),
	}
	res.freeBytes <- bufferSize
	return res
}

func (r *readerPump) start() {
	go r.readMessagesLoop()
	go r.dataRequestLoop()
}

func (r *readerPump) readMessagesLoop() {
	for {
		serverMessage, err := r.stream.Recv()
		if err != nil {
			r.close(err)
			return
		}

		status := serverMessage.StatusData()
		if status.Status != pqstreamreader.StatusOk {
			// TODO: actualize error message
			r.close(xerrors.WithStackTrace(fmt.Errorf("bad status from pq grpc stream: %v", status.Status)))
		}

		switch m := serverMessage.(type) {
		case *pqstreamreader.ReadResponse:
			r.onReadResponse(m)
		case *pqstreamreader.StartPartitionSessionRequest:

		default:
			r.close(xerrors.WithStackTrace(fmt.Errorf("receive unexpected message: %#v (%v)", m, reflect.TypeOf(m))))
		}
	}
}

func (r *readerPump) dataRequestLoop() {
	if r.ctx.Err() != nil {
		return
	}

	doneChan := r.ctx.Done()

	for {
		select {
		case <-doneChan:
			r.close(r.ctx.Err())
			return
		case free := <-r.freeBytes:
			err := r.stream.Send(&pqstreamreader.ReadRequest{BytesSize: free})
			if err != nil {
				r.close(err)
			}
		}
	}
}

func (r *readerPump) dataParseLoop() {
	for {
		select {
		case <-r.ctx.Done():
			r.close(r.ctx.Err())
			return

		case <-r.readResponsesParseSignal:
			// start work
		}

	consumeReadResponseBuffer:
		for {
			resp := r.getFirstReadResponse()
			if resp == nil {
				// buffer is empty, need wait new message
				break consumeReadResponseBuffer
			} else {
				r.dataParse(resp)
			}
		}
	}
}

func (r *readerPump) getFirstReadResponse() (res *pqstreamreader.ReadResponse) {
	r.m.Lock()
	defer r.m.Unlock()

	if len(r.readResponses) > 0 {
		res = r.readResponses[0]

		copy(r.readResponses, r.readResponses[1:])
		r.readResponses = r.readResponses[:len(r.readResponses)-1]
	}

	return res
}

func (r *readerPump) dataParse(mess *pqstreamreader.ReadResponse) {
	panic("not implemented")
}

func (r *readerPump) close(err error) {
	r.setErr.Do(func() {
		r.err = err
		r.cancel()
	})
}

func (r *readerPump) onReadResponse(mess *pqstreamreader.ReadResponse) {
	r.m.Lock()
	defer r.m.Unlock()

	r.readResponses = append(r.readResponses, mess)
	select {
	case r.readResponsesParseSignal <- struct{}{}:
	default:
		// no blocking
	}
}

func (r *readerPump) onStartPartitionSessionRequest(mess *pqstreamreader.StartPartitionSessionRequest) {
	// TODO: improve handler
	err := r.stream.Send(&pqstreamreader.StartPartitionSessionResponse{PartitionSessionID: mess.PartitionSession.PartitionSessionID})
	if err != nil {
		r.close(err)
	}
}

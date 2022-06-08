package pq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ictx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errGracefulShutdownPartition = xerrors.Wrap(errors.New("graceful shutdown partition"))
var errPartitionStopped = xerrors.Wrap(errors.New("partition stopped"))

type partitionSessionID = pqstreamreader.PartitionSessionID

type readerPump struct {
	ctx    context.Context
	cancel ictx.CancelErrFunc

	freeBytes         chan int
	stream            ReaderStream
	sessionController pumpSessionController

	readResponsesParseSignal chan struct{}
	messageBatches           chan *Batch
	cred                     credentials.Credentials
	credUpdateInterval       time.Duration

	m             sync.RWMutex
	err           error
	started       bool
	readResponses []*pqstreamreader.ReadResponse // use slice instead channel for guarantee read grpc stream without block
}

func newReaderPump(stopPump context.Context, bufferSize int, stream ReaderStream, cred credentials.Credentials, credUpdateInterval time.Duration) *readerPump {
	stopPump, cancel := ictx.WithErrCancel(stopPump)
	res := &readerPump{
		ctx:                      stopPump,
		freeBytes:                make(chan int, 1),
		stream:                   stream,
		cancel:                   cancel,
		cred:                     cred,
		credUpdateInterval:       credUpdateInterval,
		readResponsesParseSignal: make(chan struct{}, 1),
		messageBatches:           make(chan *Batch),
	}
	res.sessionController.init(res.ctx, res)
	res.freeBytes <- bufferSize
	return res
}

func (r *readerPump) ReadMessageBatch(ctx context.Context) (*Batch, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if r.ctx.Err() != nil {
		return nil, r.ctx.Err()
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, ctx.Err()
	case batch := <-r.messageBatches:
		r.freeBytes <- batch.sizeBytes
		return batch, nil
	}
}

func (r *readerPump) Commit(ctx context.Context, offset CommitBatch) error {
	req := &pqstreamreader.CommitOffsetRequest{
		PartitionsOffsets: offset.toPartitionsOffsets(),
	}
	return r.stream.Send(req)
}

func (r *readerPump) send(mess pqstreamreader.ClientMessage) error {
	err := r.stream.Send(mess)
	if err != nil {
		r.close(err)
	}
	return err
}

func (r *readerPump) Start() error {
	if err := r.setStarted(); err != nil {
		return err
	}

	if err := r.initSession(); err != nil {
		r.close(err)
	}

	go r.readMessagesLoop()
	go r.dataRequestLoop()
	go r.dataParseLoop()
	go r.updateTokenLoop()
	return nil
}

func (r *readerPump) setStarted() error {
	r.m.Lock()
	defer r.m.Unlock()

	if r.started {
		return xerrors.WithStackTrace(errors.New("already started"))
	}

	r.started = true
	return nil
}

func (r *readerPump) initSession() error {
	mess := pqstreamreader.InitRequest{
		TopicsReadSettings: []pqstreamreader.TopicReadSettings{
			{
				Topic: "/local/asd",
			},
		},
		Consumer:           "test",
		MaxLagDuration:     0,
		StartFromWrittenAt: time.Time{},
		SessionID:          "",
		ConnectionAttempt:  0,
		State:              pqstreamreader.State{},
		IdleTimeoutMs:      0,
	}

	if err := r.stream.Send(&mess); err != nil {
		return err
	}

	resp, err := r.stream.Recv()
	if err != nil {
		return err
	}

	if status := resp.StatusData(); status.Status != pqstreamreader.StatusSuccess {
		return xerrors.WithStackTrace(fmt.Errorf("bad status on initial error: %v (%v)", status.Status, status.Issues))
	}

	_, ok := resp.(*pqstreamreader.InitResponse)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf("bad message type on session init: %v (%v)", resp, reflect.TypeOf(resp)))
	}

	// TODO: log session id
	return nil
}

func (r *readerPump) readMessagesLoop() {
	for {
		serverMessage, err := r.stream.Recv()
		if err != nil {
			r.close(err)
			return
		}

		status := serverMessage.StatusData()
		if status.Status != pqstreamreader.StatusSuccess {
			// TODO: actualize error message
			r.close(xerrors.WithStackTrace(fmt.Errorf("bad status from pq grpc stream: %v", status.Status)))
		}

		switch m := serverMessage.(type) {
		case *pqstreamreader.ReadResponse:
			r.onReadResponse(m)
		case *pqstreamreader.StartPartitionSessionRequest:
			if err = r.sessionController.onStartPartitionSessionRequest(m); err != nil {
				r.close(err)
				return
			}
		case *pqstreamreader.StopPartitionSessionRequest:
			if err = r.sessionController.onStopPartitionSessionRequest(m); err != nil {
				r.close(err)
				return
			}
		case *pqstreamreader.CommitOffsetResponse:
			if err = r.onCommitResponse(m); err != nil {
				r.close(err)
				return
			}

		case *pqstreamreader.PartitionSessionStatusResponse:
			if err = r.sessionController.onPartitionStatusResponse(m); err != nil {
				r.close(err)
			}
			return

		case *pqstreamreader.UpdateTokenResponse:
			// skip
		default:
			// TODO: remove before release
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

func (r *readerPump) updateTokenLoop() {
	ticker := time.NewTicker(r.credUpdateInterval)
	defer ticker.Stop()

	readerCancel := r.ctx.Done()
	for {
		select {
		case <-readerCancel:
			return
		case <-ticker.C:
			tokenCtx, cancel := context.WithCancel(r.ctx)
			err := r.updateToken(tokenCtx)
			cancel()
			if err != nil {
				// TODO: log
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
	batchesCount := 0
	for i := range mess.Partitions {
		batchesCount += len(mess.Partitions[i].Batches)
	}

	doneChannel := r.ctx.Done()
	for pIndex := range mess.Partitions {
		p := &mess.Partitions[pIndex]
		session := &PartitionSession{
			PartitionID: -1,
			ID:          p.PartitionSessionID,
		}
		for bIndex := range p.Batches {
			select {
			case r.messageBatches <- NewBatchFromStream(context.TODO(), "topic-todo", session, p.Batches[bIndex]):
				// pass
			case <-doneChannel:
				return
			}
		}
	}
}

func (r *readerPump) close(err error) {
	r.m.Lock()
	defer r.m.Lock()

	if r.err != nil {
		return
	}

	r.err = err
	r.cancel(err)
}

func (r *readerPump) onCommitResponse(mess *pqstreamreader.CommitOffsetResponse) error {
	for i := range mess.Committed {
		commit := &mess.Committed[i]
		err := r.sessionController.sessionModify(commit.PartitionSessionID, func(p *partitionSessionData) {
			p.commitOffsetNotify(commit.Committed, nil)
		})
		if err != nil {
			return err
		}
	}

	return nil
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

func (r *readerPump) updateToken(ctx context.Context) error {
	token, err := r.cred.Token(ctx)
	if err != nil {
		// TODO: log
		return xerrors.WithStackTrace(err)
	}

	err = r.send(&pqstreamreader.UpdateTokenRequest{Token: token})
	if err != nil {
		return err
	}
	return nil
}

type partitionSessionData struct {
	Topic       string
	PartitionID int64

	graceful       context.Context
	gracefulCancel ictx.CancelErrFunc
	alive          context.Context
	aliveCancel    ictx.CancelErrFunc
	commitWaiters  []commitWaiter
}

func newPartitionSessionData(readerCtx context.Context, mess *pqstreamreader.StartPartitionSessionRequest) *partitionSessionData {
	res := &partitionSessionData{
		Topic:       mess.PartitionSession.Topic,
		PartitionID: mess.PartitionSession.PartitionID,
	}

	res.graceful, res.gracefulCancel = ictx.WithErrCancel(context.Background())
	res.alive, res.aliveCancel = ictx.WithErrCancel(readerCtx)
	return res
}

func (p *partitionSessionData) commitOffsetNotify(offset pqstreamreader.Offset, err error) {
	newWaiters := p.commitWaiters[:0]
	for i := range p.commitWaiters {
		waiter := &p.commitWaiters[i]
		if waiter.offset <= offset {
			waiter.notify(err)
		} else {
			newWaiters = append(newWaiters, *waiter)
		}
	}
	p.commitWaiters = newWaiters
}
func (p *partitionSessionData) nofityGraceful() {
	p.gracefulCancel(errGracefulShutdownPartition)
}

func (p *partitionSessionData) close(err error) {
	p.aliveCancel(err)
	for _, waiter := range p.commitWaiters {
		waiter.notify(err)
	}
}

func (p *partitionSessionData) onStatusResponse(m *pqstreamreader.PartitionSessionStatusResponse) {
	// TODO: response to status waiters?
}

type commitWaiter struct {
	offset pqstreamreader.Offset
	notify func(error)
}

type pumpSessionController struct {
	ctx context.Context
	r   *readerPump

	m        sync.RWMutex
	sessions map[partitionSessionID]*partitionSessionData
}

func (c *pumpSessionController) init(ctx context.Context, reader *readerPump) {
	c.ctx = ctx
	c.r = reader
	c.sessions = make(map[partitionSessionID]*partitionSessionData)
}

func (c *pumpSessionController) requestStatus(id partitionSessionID) error {
	if _, ok := c.sessions[id]; !ok {
		return xerrors.WithStackTrace(fmt.Errorf("unexpected session id: %v", id))
	}

	return c.r.send(&pqstreamreader.PartitionSessionStatusRequest{PartitionSessionID: id})
}

func (c *pumpSessionController) onStartPartitionSessionRequest(mess *pqstreamreader.StartPartitionSessionRequest) error {
	// TODO: improve handler
	// TODO: add user handler

	data := newPartitionSessionData(c.ctx, mess)

	if err := c.sessionAdd(mess.PartitionSession.PartitionSessionID, data); err != nil {
		return err
	}

	return c.r.send(&pqstreamreader.StartPartitionSessionResponse{PartitionSessionID: mess.PartitionSession.PartitionSessionID})
}

func (c *pumpSessionController) onStopPartitionSessionRequest(mess *pqstreamreader.StopPartitionSessionRequest) error {
	if mess.Graceful {
		err := c.sessionModify(mess.PartitionSessionID, func(p *partitionSessionData) {
			p.nofityGraceful()
		})
		return err
	}

	if data, err := c.sessionDel(mess.PartitionSessionID); err == nil {
		data.close(errPartitionStopped)
	} else {
		return err
	}

	return nil
}

func (c *pumpSessionController) sessionAdd(id partitionSessionID, data *partitionSessionData) error {
	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.sessions[id]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", id))
	}
	c.sessions[id] = data
	return nil
}

func (c *pumpSessionController) sessionDel(id partitionSessionID) (*partitionSessionData, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if data, ok := c.sessions[id]; ok {
		delete(c.sessions, id)
		return data, nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("delete undefined partition session: %v", id))
}
func (c *pumpSessionController) sessionModify(id partitionSessionID, callback func(p *partitionSessionData)) error {
	c.m.Lock()
	defer c.m.Unlock()

	if p, ok := c.sessions[id]; ok {
		callback(p)
		return nil
	}

	return xerrors.WithStackTrace(fmt.Errorf("modify unexpectet session id: %v", id))
}

func (c *pumpSessionController) onPartitionStatusResponse(m *pqstreamreader.PartitionSessionStatusResponse) error {
	return c.sessionModify(m.PartitionSessionID, func(p *partitionSessionData) {
		p.onStatusResponse(m)
	})
}

func TestCreatePump(ctx context.Context, stream ReaderStream, cred credentials.Credentials, interval time.Duration) *readerPump {
	return newReaderPump(ctx, 1024*1024*1024, stream, cred, interval)
}

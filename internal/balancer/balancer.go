package balancer

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	discoveryBuilder "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var ErrClusterEmpty = xerrors.Wrap(fmt.Errorf("cluster empty"))

type balancer struct {
	driverConfig      config.Config
	balancerConfig    balancerConfig.Config
	pool              *conn.Pool
	discovery         discovery.Client
	discoveryRepeater repeater.Repeater
	localDCDetector   func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

	m                sync.RWMutex
	connectionsState *connectionsState
}

func (b *balancer) clusterDiscovery(ctx context.Context) (err error) {
	var (
		onDone = trace.DriverOnBalancerUpdate(
			b.driverConfig.Trace(),
			&ctx,
			b.balancerConfig.DetectlocalDC,
		)
		endpoints []endpoint.Endpoint
		localDC   string
	)

	defer func() {
		nodes := make([]trace.EndpointInfo, 0, len(endpoints))
		for _, e := range endpoints {
			nodes = append(nodes, e.Copy())
		}
		onDone(
			nodes,
			localDC,
			err,
		)
	}()

	endpoints, err = b.discovery.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	if b.balancerConfig.DetectlocalDC {
		localDC, err = b.localDCDetector(ctx, endpoints)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}

	b.applyDiscoveredEndpoints(ctx, endpoints, localDC)

	return nil
}

func (b *balancer) applyDiscoveredEndpoints(ctx context.Context, endpoints []endpoint.Endpoint, localDC string) {
	connections := endpointsToConnections(b.pool, endpoints)
	for _, c := range connections {
		b.pool.Allow(ctx, c)
	}

	info := balancerConfig.Info{SelfLocation: localDC}
	state := newConnectionsState(connections, b.balancerConfig.IsPreferConn, info, b.balancerConfig.AllowFalback)

	b.m.Lock()
	defer b.m.Unlock()

	b.connectionsState = state
}

func (b *balancer) Discovery() discovery.Client {
	return b.discovery
}

func (b *balancer) Close(ctx context.Context) (err error) {
	onDone := trace.DriverOnBalancerClose(
		b.driverConfig.Trace(),
		&ctx,
	)
	defer func() {
		onDone(err)
	}()

	issues := make([]error, 0, 2)

	if b.discoveryRepeater != nil {
		b.discoveryRepeater.Stop()
	}

	if err = b.discovery.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("balancer close failed", issues...))
	}

	return nil
}

func New(
	ctx context.Context,
	c config.Config,
	pool *conn.Pool,
	opts ...discoveryConfig.Option,
) (_ Connection, err error) {
	onDone := trace.DriverOnBalancerInit(
		c.Trace(),
		&ctx,
	)
	defer func() {
		onDone(err)
	}()

	b := &balancer{
		driverConfig:    c,
		pool:            pool,
		localDCDetector: detectLocalDC,
	}

	if config := c.Balancer(); config == nil {
		b.balancerConfig = balancerConfig.Config{}
	} else {
		b.balancerConfig = *config
	}

	discoveryEndpoint := endpoint.New(c.Endpoint())
	discoveryConnection := pool.Get(discoveryEndpoint)

	discoveryConfig := discoveryConfig.New(opts...)

	b.discovery = discoveryBuilder.New(
		discoveryConnection,
		discoveryConfig,
	)

	if b.balancerConfig.SingleConn {
		b.connectionsState = newConnectionsState(
			endpointsToConnections(pool, []endpoint.Endpoint{discoveryEndpoint}),
			nil, balancerConfig.Info{}, false)
	} else {
		if err = b.clusterDiscovery(ctx); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		if d := discoveryConfig.Interval(); d > 0 {
			b.discoveryRepeater = repeater.New(d, func(ctx context.Context) (err error) {
				ctx, cancel := context.WithTimeout(ctx, d)
				defer cancel()

				return b.clusterDiscovery(ctx)
			},
				repeater.WithName("discovery"),
				repeater.WithTrace(b.driverConfig.Trace()),
			)
		}
	}

	var cancel context.CancelFunc
	if t := c.DialTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.DialTimeout())
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	return b, nil
}

func (b *balancer) Endpoint() string {
	return b.driverConfig.Endpoint()
}

func (b *balancer) Name() string {
	return b.driverConfig.Database()
}

func (b *balancer) Secure() bool {
	return b.driverConfig.Secure()
}

func (b *balancer) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	return b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		return cc.Invoke(ctx, method, args, reply, opts...)
	})
}

func (b *balancer) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	var client grpc.ClientStream
	err = b.wrapCall(ctx, func(ctx context.Context, cc conn.Conn) error {
		client, err = cc.NewStream(ctx, desc, method, opts...)
		return err
	})
	if err == nil {
		return client, nil
	}
	return nil, err
}

func (b *balancer) wrapCall(ctx context.Context, f func(ctx context.Context, cc conn.Conn) error) (err error) {
	cc, err := b.getConn(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		if err == nil {
			if cc.GetState() == conn.Banned {
				b.pool.Allow(ctx, cc)
			}
		} else {
			if xerrors.MustPessimizeEndpoint(err, b.driverConfig.ExcludeGRPCCodesForPessimization()...) {
				b.pool.Ban(ctx, cc, err)
			}
		}
	}()

	if ctx, err = b.driverConfig.Meta().Meta(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	if err = f(ctx, cc); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (b *balancer) connections() *connectionsState {
	b.m.RLock()
	defer b.m.RUnlock()

	return b.connectionsState
}

func (b *balancer) getConn(ctx context.Context) (c conn.Conn, err error) {
	onDone := trace.DriverOnBalancerChooseEndpoint(
		b.driverConfig.Trace(),
		&ctx,
	)
	defer func() {
		if err == nil {
			onDone(c.Endpoint(), nil)
		} else {
			onDone(nil, err)
		}
	}()

	var (
		state       = b.connections()
		failedCount int
	)

	defer func() {
		if failedCount*2 > state.PreferredCount() {
			b.discoveryRepeater.Force()
		}
	}()

	c, failedCount = state.GetConnection(ctx)
	if c == nil {
		return nil, xerrors.WithStackTrace(ErrClusterEmpty)
	}
	return c, nil
}

func endpointsToConnections(p *conn.Pool, endpoints []endpoint.Endpoint) []conn.Conn {
	conns := make([]conn.Conn, 0, len(endpoints))
	for _, e := range endpoints {
		conns = append(conns, p.Get(e))
	}
	return conns
}

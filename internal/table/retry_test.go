package table

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestRetryerBackoffRetryCancelation(t *testing.T) {
	for _, testErr := range []error{
		// Errors leading to Wait repeat.
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.ResourceExhausted),
		),
		fmt.Errorf("wrap transport error: %w", xerrors.Transport(
			xerrors.WithCode(grpcCodes.ResourceExhausted),
		)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
		fmt.Errorf("wrap op error: %w", xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))),
	} {
		t.Run("", func(t *testing.T) {
			backoff := make(chan chan time.Time)
			p := SingleSession(
				simpleSession(t),
			)

			ctx, cancel := context.WithCancel(context.Background())
			results := make(chan error)
			go func() {
				err := do(
					ctx,
					p,
					config.New(),
					func(ctx context.Context, _ table.Session) error {
						return testErr
					},
					table.Options{
						FastBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
							ch := make(chan time.Time)
							backoff <- ch
							return ch
						}),
						SlowBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
							ch := make(chan time.Time)
							backoff <- ch
							return ch
						}),
					},
				)
				results <- err
			}()

			select {
			case <-backoff:
			case res := <-results:
				t.Fatalf("unexpected result: %v", res)
			}

			cancel()
		})
	}
}

func TestRetryerBadSession(t *testing.T) {
	closed := make(map[table.Session]bool)
	p := SessionProviderFunc{
		OnGet: func(ctx context.Context) (Session, error) {
			s := simpleSession(t)
			s.OnClose(func(context.Context) {
				closed[s] = true
			})
			return s, nil
		},
	}
	var (
		i          int
		maxRetryes = 100
		sessions   []table.Session
	)
	ctx, cancel := context.WithCancel(context.Background())
	err := do(
		ctx,
		p,
		config.New(),
		func(ctx context.Context, s table.Session) error {
			sessions = append(sessions, s)
			i++
			if i > maxRetryes {
				cancel()
			}
			return xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
			)
		},
		table.Options{},
	)
	if !xerrors.Is(err, context.Canceled) {
		t.Errorf("unexpected error: %v", err)
	}
	seen := make(map[table.Session]bool, len(sessions))
	for _, s := range sessions {
		if seen[s] {
			t.Errorf("session used twice")
		} else {
			seen[s] = true
		}
		if !closed[s] {
			t.Errorf("bad session was not closed")
		}
	}
}

func TestRetryerSessionClosing(t *testing.T) {
	closed := make(map[table.Session]bool)
	p := SessionProviderFunc{
		OnGet: func(ctx context.Context) (Session, error) {
			s := simpleSession(t)
			s.OnClose(func(context.Context) {
				closed[s] = true
			})
			return s, nil
		},
	}
	var sessions []table.Session
	for i := 0; i < 1000; i++ {
		err := do(
			context.Background(),
			p,
			config.New(),
			func(ctx context.Context, s table.Session) error {
				sessions = append(sessions, s)
				s.(*session).SetStatus(options.SessionClosing)
				return nil
			},
			table.Options{},
		)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	seen := make(map[table.Session]bool, len(sessions))
	for _, s := range sessions {
		if seen[s] {
			t.Errorf("session used twice")
		} else {
			seen[s] = true
		}
		if !closed[s] {
			t.Errorf("bad session was not closed")
		}
	}
}

func TestRetryerImmediateReturn(t *testing.T) {
	for _, testErr := range []error{
		xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
		),
		fmt.Errorf("wrap op error: %w", xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
		)),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.PermissionDenied),
		),
		fmt.Errorf("wrap transport error: %w", xerrors.Transport(
			xerrors.WithCode(grpcCodes.PermissionDenied),
		)),
		fmt.Errorf("whoa"),
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if e := recover(); e != nil {
					t.Fatalf("unexpected panic: %v", e)
				}
			}()
			p := SingleSession(
				simpleSession(t),
			)
			err := do(
				context.Background(),
				p,
				config.New(),
				func(ctx context.Context, _ table.Session) error {
					return testErr
				},
				table.Options{
					FastBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
						panic("this code will not be called")
					}),
					SlowBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
						panic("this code will not be called")
					}),
				},
			)
			if !xerrors.Is(err, testErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// We are testing all suspentions of custom operation func against to all deadline
// timeouts - all sub-tests must have latency less than timeouts (+tolerance)
func TestRetryContextDeadline(t *testing.T) {
	timeouts := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
	}
	sleeps := []time.Duration{
		time.Nanosecond,
		time.Microsecond,
		time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		5 * time.Second,
	}
	errs := []error{
		io.EOF,
		context.DeadlineExceeded,
		fmt.Errorf("test error"),
		xerrors.Transport(),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.Canceled),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unknown),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.InvalidArgument),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.DeadlineExceeded),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.NotFound),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.AlreadyExists),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.PermissionDenied),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.ResourceExhausted),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.FailedPrecondition),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.Aborted),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.OutOfRange),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unimplemented),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.Internal),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unavailable),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.DataLoss),
		),
		xerrors.Transport(
			xerrors.WithCode(grpcCodes.Unauthenticated),
		),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_INTERNAL_ERROR)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SCHEME_ERROR)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_TIMEOUT)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ALREADY_EXISTS)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_CANCELLED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNDETERMINED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNSUPPORTED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY)),
	}
	client := &Client{
		cc: testutil.NewRouter(testutil.WithInvokeHandlers(testutil.InvokeHandlers{})),
	}
	p := SessionProviderFunc{
		OnGet: client.createSession,
	}
	r := xrand.New(xrand.WithLock())
	for i := range timeouts {
		for j := range sleeps {
			timeout := timeouts[i]
			sleep := sleeps[j]
			t.Run(fmt.Sprintf("Timeout=%v,Sleep=%v", timeout, sleep), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_ = do(
					ctx,
					p,
					config.New(),
					func(ctx context.Context, _ table.Session) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(sleep):
							return errs[r.Int(len(errs))]
						}
					},
					table.Options{},
				)
			})
		}
	}
}

type CustomError struct {
	Err error
}

func (e *CustomError) Error() string {
	return fmt.Sprintf("custom error: %v", e.Err)
}

func (e *CustomError) Unwrap() error {
	return e.Err
}

func TestRetryWithCustomErrors(t *testing.T) {
	var (
		limit = 10
		ctx   = context.Background()
		p     = SessionProviderFunc{
			OnGet: func(ctx context.Context) (Session, error) {
				return simpleSession(t), nil
			},
		}
	)
	for _, test := range []struct {
		error         error
		retriable     bool
		deleteSession bool
	}{
		{
			error: &CustomError{
				Err: retry.RetryableError(
					fmt.Errorf("custom error"),
					retry.WithDeleteSession(),
				),
			},
			retriable:     true,
			deleteSession: true,
		},
		{
			error: &CustomError{
				Err: xerrors.Operation(
					xerrors.WithStatusCode(
						Ydb.StatusIds_BAD_SESSION,
					),
				),
			},
			retriable:     true,
			deleteSession: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					xerrors.Operation(
						xerrors.WithStatusCode(
							Ydb.StatusIds_BAD_SESSION,
						),
					),
				),
			},
			retriable:     true,
			deleteSession: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					xerrors.Operation(
						xerrors.WithStatusCode(
							Ydb.StatusIds_UNAUTHORIZED,
						),
					),
				),
			},
			retriable:     false,
			deleteSession: false,
		},
	} {
		t.Run(test.error.Error(), func(t *testing.T) {
			var (
				i        = 0
				sessions = make(map[table.Session]int)
			)
			err := do(
				ctx,
				p,
				config.New(),
				func(ctx context.Context, s table.Session) (err error) {
					sessions[s]++
					i++
					if i < limit {
						return test.error
					}
					return nil
				},
				table.Options{},
			)
			// nolint:nestif
			if test.retriable {
				if i != limit {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
				if test.deleteSession {
					if len(sessions) != limit {
						t.Fatalf("unexpected len(sessions): %d, err: %v", len(sessions), err)
					}
					for s, n := range sessions {
						if n != 1 {
							t.Fatalf("unexpected session usage: %d, session: %v", n, s.ID())
						}
					}
				}
			} else {
				if i != 1 {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
				if len(sessions) != 1 {
					t.Fatalf("unexpected len(sessions): %d, err: %v", len(sessions), err)
				}
			}
		})
	}
}

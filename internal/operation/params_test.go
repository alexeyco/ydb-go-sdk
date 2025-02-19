package operation

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
)

func TestParams(t *testing.T) {
	for _, tt := range []struct {
		ctx                  context.Context
		preferContextTimeout bool
		timeout              time.Duration
		cancelAfter          time.Duration
		mode                 Mode
		exp                  *Ydb_Operations.OperationParams
	}{
		{
			ctx:         context.Background(),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp:         nil,
		},
		{
			ctx: WithTimeout(
				context.Background(),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithTimeout(
				WithTimeout(
					WithTimeout(
						WithTimeout(
							WithTimeout(
								context.Background(),
								time.Second*1,
							),
							time.Second*2,
						),
						time.Second*3,
					),
					time.Second*4,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithTimeout(
				WithTimeout(
					WithTimeout(
						WithTimeout(
							WithTimeout(
								context.Background(),
								time.Second*5,
							),
							time.Second*4,
						),
						time.Second*3,
					),
					time.Second*2,
				),
				time.Second*1,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithCancelAfter(
				context.Background(),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithCancelAfter(
					WithCancelAfter(
						WithCancelAfter(
							WithCancelAfter(
								context.Background(),
								time.Second*1,
							),
							time.Second*2,
						),
						time.Second*3,
					),
					time.Second*4,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithCancelAfter(
				WithCancelAfter(
					WithCancelAfter(
						WithCancelAfter(
							WithCancelAfter(
								context.Background(),
								time.Second*5,
							),
							time.Second*4,
						),
						time.Second*3,
					),
					time.Second*2,
				),
				time.Second*1,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				CancelAfter: durationpb.New(time.Second * 1),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        0,
			exp: &Ydb_Operations.OperationParams{
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     0,
			cancelAfter: 0,
			mode:        ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: WithCancelAfter(
				WithTimeout(
					context.Background(),
					time.Second*5,
				),
				time.Second*5,
			),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeAsync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_ASYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				// nolint: govet
				ctx, _ := context.WithTimeout(WithCancelAfter(
					WithTimeout(
						context.Background(),
						time.Second*5,
					),
					time.Second*5,
				), time.Second*10)
				return ctx
			}(),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeAsync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_ASYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				// nolint: govet
				ctx, _ := context.WithTimeout(WithCancelAfter(
					WithTimeout(
						context.Background(),
						time.Second*5,
					),
					time.Second*5,
				), time.Second*1)
				return ctx
			}(),
			timeout:     time.Second * 2,
			cancelAfter: 0,
			mode:        ModeAsync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_ASYNC,
				OperationTimeout: durationpb.New(time.Second * 5),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				// nolint: govet
				ctx, _ := context.WithTimeout(WithCancelAfter(
					WithTimeout(
						context.Background(),
						time.Second*5,
					),
					time.Second*5,
				), time.Second*1)
				return ctx
			}(),
			preferContextTimeout: true,
			timeout:              time.Second * 2,
			cancelAfter:          0,
			mode:                 ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 1),
				CancelAfter:      durationpb.New(time.Second * 5),
			},
		},
		{
			ctx: func() context.Context {
				// nolint: govet
				ctx, _ := context.WithTimeout(context.Background(), time.Second*1)
				return ctx
			}(),
			preferContextTimeout: true,
			timeout:              time.Second * 2,
			cancelAfter:          0,
			mode:                 ModeSync,
			exp: &Ydb_Operations.OperationParams{
				OperationMode:    Ydb_Operations.OperationParams_SYNC,
				OperationTimeout: durationpb.New(time.Second * 1),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := Params(tt.ctx, tt.timeout, tt.cancelAfter, tt.mode)
			t.Logf(
				"Params(): {%v}, compare to: {%v}",
				got,
				tt.exp,
			)
			if !tt.preferContextTimeout {
				if !reflect.DeepEqual(got, tt.exp) {
					t.Errorf(
						"Params(): {%v}, want: {%v}",
						got,
						tt.exp,
					)
				}
				return
			}
			if !reflect.DeepEqual(got.OperationMode, tt.exp.OperationMode) {
				t.Errorf(
					"Params().OperationMode: %v, want: %v",
					got.OperationMode,
					tt.exp.OperationMode,
				)
			}
			if !reflect.DeepEqual(got.CancelAfter, tt.exp.CancelAfter) {
				t.Errorf(
					"Params().CancelAfter: %v, want: %v",
					got.CancelAfter.AsDuration(),
					tt.exp.CancelAfter.AsDuration(),
				)
			}
			if got.OperationTimeout.AsDuration() > tt.exp.OperationTimeout.AsDuration() {
				t.Errorf(
					"Params().OperationTimeout: %v, want: <= %v",
					got.OperationTimeout.AsDuration(),
					tt.exp.OperationTimeout.AsDuration(),
				)
			}
		})
	}
}

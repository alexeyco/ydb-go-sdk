package pq_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"

	"github.com/ydb-platform/ydb-go-sdk/v3/pq"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestInit(t *testing.T) {
	ctx := context.Background()
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=/local")
	defer func() { _ = db.Close(ctx) }()
	require.NoError(t, err)

	grpcConn := db.(grpc.ClientConnInterface)
	pqClient := Ydb_PersQueue_V12.NewPersQueueServiceClient(grpcConn)
	grpcStream, err := pqClient.StreamingRead(ctx)
	require.NoError(t, err)

	pump := pq.TestCreatePump(ctx, pqstreamreader.StreamReader{Stream: grpcStream})
	require.NoError(t, pump.Start())
	batch, err := pump.ReadMessageBatch(ctx)
	require.NoError(t, err)
	for _, mess := range batch.Messages {
		data, err := io.ReadAll(mess.Data)
		require.NoError(t, err)
		t.Log(string(data))
	}
	time.Sleep(time.Second)
}

package xerrors

import (
	"errors"
	"fmt"
	"testing"

	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestIsYdb(t *testing.T) {
	for _, test := range []struct {
		error      error
		isYdbError bool
	}{
		{
			error:      nil,
			isYdbError: false,
		},
		{
			error:      Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			isYdbError: true,
		},
		{
			error:      Transport(WithCode(grpcCodes.DeadlineExceeded)),
			isYdbError: true,
		},
		{
			error:      RetryableError(fmt.Errorf("")),
			isYdbError: false,
		},
		{
			error:      WithStackTrace(Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(Transport(WithCode(grpcCodes.DeadlineExceeded))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(RetryableError(fmt.Errorf(""))),
			isYdbError: false,
		},
		{
			error:      WithStackTrace(WithStackTrace(Operation(WithStatusCode(Ydb.StatusIds_BAD_SESSION)))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(WithStackTrace(Transport(WithCode(grpcCodes.DeadlineExceeded)))),
			isYdbError: true,
		},
		{
			error:      WithStackTrace(WithStackTrace(RetryableError(fmt.Errorf("")))),
			isYdbError: false,
		},
		{
			error:      fmt.Errorf("TestError%s", "Printf"),
			isYdbError: false,
		},
		{
			error:      errors.New("TestError"),
			isYdbError: false,
		},
		{
			error:      Wrap(fmt.Errorf("TestError%s", "Printf")),
			isYdbError: true,
		},
		{
			error:      Wrap(errors.New("TestError")),
			isYdbError: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			if IsYdb(test.error) != test.isYdbError {
				t.Fatalf("unexpected check ydb error: %v, want: %v", IsYdb(test.error), test.isYdbError)
			}
		})
	}
}

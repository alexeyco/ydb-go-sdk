package ydb_test

import (
	"context"
	"fmt"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

func Example_table() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/?database=/local")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		query = `SELECT 42 as id, "my string" as myStr`
		id    int32  // required value
		myStr string // optional value
	)
	err = db.Table().Do( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, nil)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer res.Close()                                // cleanup resources
			if err = res.NextResultSetErr(ctx); err != nil { // check single result set and switch to it
				return err // for auto-retry with driver
			}
			for res.NextRow() { // iterate over rows
				err = res.ScanNamed(
					named.Required("id", &id),
					named.OptionalWithDefault("myStr", &myStr),
				)
				if err != nil {
					return err // generally scan error not retryable, return it for driver check error
				}
				log.Printf("id=%v, myStr='%s'\n", id, myStr)
			}
			return res.Err() // return finally result error for auto-retry with driver
		},
	)
	if err != nil {
		log.Printf("unexpected error: %v", err)
	}
}

func Example_scripting() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/?database=/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err := db.Scripting().Execute(
			ctx,
			"SELECT 1+1",
			table.NewQueryParameters(),
		)
		if err != nil {
			return err
		}
		defer res.Close() // cleanup resources
		if !res.NextResultSet(ctx) {
			return retry.RetryableError(
				fmt.Errorf("no result sets"),
				retry.WithBackoff(retry.TypeNoBackoff),
			)
		}
		if !res.NextRow() {
			return retry.RetryableError(
				fmt.Errorf("no rows"),
				retry.WithBackoff(retry.TypeSlowBackoff),
			)
		}
		var sum int32
		if err = res.Scan(&sum); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		if sum != 2 {
			return fmt.Errorf("unexpected sum: %v", sum)
		}
		return res.Err()
	}, retry.WithIdempotent(true)); err != nil {
		fmt.Printf("Execute failed: %v", err)
	}
}

func Example_discovery() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/?database=/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	endpoints, err := db.Discovery().Discover(ctx)
	if err != nil {
		fmt.Printf("discover failed: %v", err)
		return
	}
	fmt.Printf("%s endpoints:\n", db.Name())
	for i, e := range endpoints {
		fmt.Printf("%d) %s\n", i, e.String())
	}
}

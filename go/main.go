package main

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go/pkg/client"
)

func main() {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000")
	fmt.Println(err)

	/*q1 := cl.TimeTableQuery(time.Second, time.Now()).Update("foo = i % 10").Tail(5).Where("foo < 5")
	q2 := client.MergeQuery("", q1, q1)

	tbls, err := cl.ExecBatch(ctx, q2)
	fmt.Println(err)
	tbl := tbls[0]*/

	tbl, err := cl.OpenTable(ctx, "t")
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = cl.Subscribe(ctx, tbl)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = tbl.Release(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
}

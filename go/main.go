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

	tbl, err := cl.OpenTable(ctx, "t")
	if err != nil {
		fmt.Println(err)
		return
	}

	tickingTbl, updateChan, err := cl.Subscribe(ctx, tbl)
	if err != nil {
		fmt.Println(err)
		return
	}

	for update := range updateChan {
		tickingTbl.ApplyUpdate(update)
		fmt.Println(tickingTbl)
	}

	err = tbl.Release(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
}

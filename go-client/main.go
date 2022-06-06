package main

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go-client/client"
)

func main() {
	ctx := context.Background()

	s, err := client.NewClient(ctx, "localhost", "10000")
	if err != nil {
		fmt.Println("Session err:", err)
		return;
	}
	defer s.Close()

	tbl, err := s.EmptyTable(ctx, 10)
	if err != nil {
		fmt.Println("EmptyTable err:", err)
		return;
	}

	err = s.BindToVariable(ctx, "gotest", tbl)
	if err != nil {
		fmt.Println("BindToVariable err:", err)
		return;
	}
}

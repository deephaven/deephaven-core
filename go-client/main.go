package main

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go-client/session"
)

func main() {
	ctx := context.Background()

	s, err := session.NewSession(ctx, "localhost", "10000")
	if err != nil {
		fmt.Println("Session err:", err)
	}
	defer s.Close(ctx)
}

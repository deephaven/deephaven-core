package client_test

import (
	"context"
	"testing"

	"github.com/deephaven/deephaven-core/go/client/client"
	"github.com/deephaven/deephaven-core/go/client/internal/test_tools"
)

func TestOpenTable(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), client.WithConsole("python"))
	test_tools.CheckError(t, "NewClient", err)

	err = c.RunScript(ctx,
		`
from deephaven import empty_table
gotesttable = empty_table(42)
`)
	test_tools.CheckError(t, "RunScript", err)

	tbl, err := c.OpenTable(ctx, "gotesttable")
	test_tools.CheckError(t, "OpenTable", err)
	defer tbl.Release(ctx)

	rec, err := tbl.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer rec.Release()

	if rec.NumCols() != 0 || rec.NumRows() != 42 {
		t.Error("table had wrong size")
		return
	}
}

func TestNoConsole(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	err = c.RunScript(ctx, "print('hi')")
	if err != client.ErrNoConsole {
		t.Error("wrong or missing RunScript error", err)
		return
	}

	tbl, err := c.EmptyTable(ctx, 10)
	test_tools.CheckError(t, "EmptyTable", err)

	err = c.BindToVariable(ctx, "this_should_fail", tbl)
	if err != client.ErrNoConsole {
		t.Error("wrong or missing BindToVariable error", err)
	}
}

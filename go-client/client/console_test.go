package client_test

import (
	"context"
	"testing"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_setup"
)

func TestOpenTable(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	test_setup.CheckError(t, "NewClient", err)

	err = c.RunScript(ctx,
		`
from deephaven import empty_table
gotesttable = empty_table(42)
`)
	test_setup.CheckError(t, "RunScript", err)

	tbl, err := c.OpenTable(ctx, "gotesttable")
	test_setup.CheckError(t, "OpenTable", err)
	defer tbl.Release(ctx)

	rec, err := tbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer rec.Release()

	if rec.NumCols() != 0 || rec.NumRows() != 42 {
		t.Error("table had wrong size")
		return
	}
}

package client_test

import (
	"context"
	"testing"

	"github.com/deephaven/deephaven-core/go-client/client"
)

func TestEmptyTable(t *testing.T) {
	var expectedRows int64 = 5
	var expectedCols int64 = 0

	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000")
	if err != nil {
		t.Errorf("NewClient err %e", err)
	}
	defer c.Close()

	tbl, err := c.EmptyTable(ctx, expectedRows)
	if err != nil {
		t.Errorf("EmptyTable err %e", err)
	}

	rec, err := tbl.Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot err %e", err)
	}
	defer rec.Release()

	rows, cols := rec.NumRows(), rec.NumCols()
	if rows != expectedRows || cols != expectedCols {
		t.Errorf("Record had wrong size (expected %d x %d, got %d x %d)", expectedRows, expectedCols, rows, cols)
	}
}

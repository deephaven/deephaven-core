package client_test

import (
	"context"
	"testing"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_setup"
)

func TestUpdateDropQuery(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	input := test_setup.ExampleRecord()
	defer input.Release()

	before, err := c.ImportTable(ctx, input)
	if err != nil {
		t.Errorf("ImportTable %s", err.Error())
		return
	}

	after, err := before.Query().
		Update([]string{"Foo = Close * 17.0", "Bar = Vol + 1"}).
		DropColumns([]string{"Bar", "Ticker"}).
		Execute(ctx)

	if err != nil {
		t.Errorf("DropColumns %s", err.Error())
		return
	}

	result, err := after.Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	if result.NumCols() != 3 {
		t.Errorf("wrong number of columns %d", result.NumCols())
		return
	}

	col0, col1, col2 := result.ColumnName(0), result.ColumnName(1), result.ColumnName(2)
	if col0 != "Close" || col1 != "Vol" || col2 != "Foo" {
		t.Errorf("wrong columns %s %s %s", col0, col1, col2)
		return
	}
}

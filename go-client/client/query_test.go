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

	updateQuery := before.Query().Update([]string{"Foo = Close * 17.0", "Bar = Vol + 1"})
	dropQuery := updateQuery.DropColumns([]string{"Bar", "Ticker"})

	tables, err := c.ExecQuery(ctx, []client.QueryNode{updateQuery, dropQuery})
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return
	}
	if len(tables) != 2 {
		t.Errorf("wrong number of result tables")
		return
	}

	updTbl, err := tables[0].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}
	drpTbl, err := tables[1].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	if updTbl.NumCols() != 5 {
		t.Errorf("wrong number of columns %d", updTbl.NumCols())
	}

	if drpTbl.NumCols() != 3 {
		t.Errorf("wrong number of columns %d", drpTbl.NumCols())
		return
	}

	col0, col1, col2 := drpTbl.ColumnName(0), drpTbl.ColumnName(1), drpTbl.ColumnName(2)
	if col0 != "Close" || col1 != "Vol" || col2 != "Foo" {
		t.Errorf("wrong columns %s %s %s", col0, col1, col2)
		return
	}
}

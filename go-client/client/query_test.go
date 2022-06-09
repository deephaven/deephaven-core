package client_test

import (
	"context"
	"testing"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_setup"
)

func TestSeparateQueries(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	left := c.EmptyTableQuery(123)
	right := c.TimeTableQuery(10000000, nil)

	tables, err := c.ExecQuery(ctx, left, right)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return
	}
	if len(tables) != 2 {
		t.Errorf("wrong number of tables")
		return
	}

	leftTbl, err := tables[0].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	rightTbl, err := tables[1].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	if leftTbl.NumCols() != 0 {
		t.Errorf("wrong left table")
		return
	}
	if rightTbl.NumCols() != 1 {
		t.Errorf("wrong right table")
		return
	}
}

func TestEmptyTableQuery(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	base := c.EmptyTableQuery(123)

	derived := base.Update([]string{"a = ii"})

	tables, err := c.ExecQuery(ctx, base, derived)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return
	}
	if len(tables) != 2 {
		t.Errorf("wrong number of tables")
		return
	}

	baseTbl, err := tables[0].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	derivedTbl, err := tables[1].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	if baseTbl.NumRows() != 123 || baseTbl.NumCols() != 0 {
		t.Errorf("base table had wrong size")
		return
	}

	if derivedTbl.NumRows() != 123 || derivedTbl.NumCols() != 1 {
		t.Errorf("derived table had wrong size")
		return
	}
}

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

	tables, err := c.ExecQuery(ctx, updateQuery, dropQuery)
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

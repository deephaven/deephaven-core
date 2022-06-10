package client_test

import (
	"context"
	"sort"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_setup"
)

func TestDagQuery(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	test_setup.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_setup.ExampleRecord()
	defer rec.Release()

	// Close (float32), Vol (int32), Ticker (string)
	exTable, err := c.ImportTable(ctx, rec)
	test_setup.CheckError(t, "ImportTable", err)
	defer exTable.Release(ctx)

	// Close (float32), Vol (int32), TickerLen (int)
	exLenQuery := exTable.Query().
		Update("TickerLen = Ticker.length()").
		DropColumns("Ticker")

	// Close (float32), TickerLen (int)
	exCloseLenQuery := exLenQuery.
		Update("TickerLen = TickerLen + Vol").
		DropColumns("Vol")

	// Close (float32), TickerLen (int)
	otherQuery := c.EmptyTableQuery(5).
		Update("Close = (float)(ii / 3.0)", "TickerLen = (int)(ii + 1)")

	// Close (float32), TickerLen (int)
	finalQuery := otherQuery.Merge("", exCloseLenQuery)

	tables, err := c.ExecQuery(ctx, finalQuery, otherQuery, exCloseLenQuery, exLenQuery)
	test_setup.CheckError(t, "ExecQuery", err)
	if len(tables) != 4 {
		t.Errorf("wrong number of tables")
		return
	}
	for i := 0; i < len(tables); i += 1 {
		defer tables[i].Release(ctx)
	}

	finalTable, err := tables[0].Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	otherTable, err := tables[1].Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	exCloseLenTable, err := tables[2].Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	exLenTable, err := tables[3].Snapshot(ctx)
	test_setup.CheckError(t, "Snapsnot", err)

	if finalTable.NumRows() != 5+7 || finalTable.NumCols() != 2 {
		t.Errorf("wrong size for finalTable")
		return
	}
	if otherTable.NumRows() != 5 || otherTable.NumCols() != 2 {
		t.Errorf("wrong size for otherTable")
		return
	}
	if exCloseLenTable.NumRows() != 7 || exCloseLenTable.NumCols() != 2 {
		t.Log(exCloseLenTable)
		t.Errorf("wrong size for exCloseLenTable")
		return
	}
	if exLenTable.NumRows() != 7 || exLenTable.NumCols() != 3 {
		t.Log(exLenTable)
		t.Errorf("wrong size for exLenTable")
		return
	}
}

func TestMergeQuery(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	test_setup.CheckError(t, "NewClient", err)
	defer c.Close()

	left, err := c.EmptyTable(ctx, 10)
	test_setup.CheckError(t, "EmptyTable", err)
	defer left.Release(ctx)

	right, err := c.EmptyTable(ctx, 5)
	test_setup.CheckError(t, "EmptyTable", err)
	defer right.Release(ctx)

	tables, err := c.ExecQuery(ctx, left.Query().Merge("", right.Query()))
	test_setup.CheckError(t, "ExecQuery", err)
	if len(tables) != 1 {
		t.Errorf("wrong number of tables")
	}
	defer tables[0].Release(ctx)

	tbl, err := tables[0].Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)

	if tbl.NumRows() != 15 || tbl.NumCols() != 0 {
		t.Errorf("table was wrong size")
	}
}

func TestSeparateQueries(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	test_setup.CheckError(t, "NewClient", err)
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
	defer tables[0].Release(ctx)
	defer tables[1].Release(ctx)

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

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	base := c.EmptyTableQuery(123)

	derived := base.Update("a = ii")

	tables, err := c.ExecQuery(ctx, base, derived)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return
	}
	if len(tables) != 2 {
		t.Errorf("wrong number of tables")
		return
	}
	defer tables[0].Release(ctx)
	defer tables[1].Release(ctx)

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

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
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
	defer before.Release(ctx)

	updateQuery := before.Query().Update("Foo = Close * 17.0", "Bar = Vol + 1")
	dropQuery := updateQuery.DropColumns("Bar", "Ticker")

	tables, err := c.ExecQuery(ctx, updateQuery, dropQuery)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return
	}
	if len(tables) != 2 {
		t.Errorf("wrong number of result tables")
		return
	}
	defer tables[0].Release(ctx)
	defer tables[1].Release(ctx)

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

type queryOp func(*client.TableHandle) []client.QueryNode

func doQueryTest(inputRec array.Record, t *testing.T, op queryOp) []array.Record {
	defer inputRec.Release()

	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	input, err := c.ImportTable(ctx, inputRec)
	if err != nil {
		t.Errorf("ImportTable %s", err.Error())
		return nil
	}
	defer input.Release(ctx)

	query := op(input)

	tables, err := c.ExecQuery(ctx, query...)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return nil
	}

	for _, table := range tables {
		defer table.Release(ctx)
	}

	var recs []array.Record
	for _, table := range tables {
		rec, err := table.Snapshot(ctx)
		if err != nil {
			t.Errorf("Snapshot %s", err.Error())
			return nil
		}
		recs = append(recs, rec)
	}

	return recs
}

func TestSort(t *testing.T) {
	results := doQueryTest(test_setup.RandomRecord(2, 10, 1000), t, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query().Sort("a"), tbl.Query().SortBy(client.SortDsc("a"))}
	})
	defer results[0].Release()
	defer results[1].Release()

	asc := results[0].Column(0).(*array.Int32).Int32Values()

	if !sort.SliceIsSorted(asc, func(i, j int) bool { return asc[i] < asc[j] }) {
		t.Error("Slice was not sorted ascending:", asc)
		return
	}

	dsc := results[1].Column(0).(*array.Int32).Int32Values()

	if !sort.SliceIsSorted(dsc, func(i, j int) bool { return dsc[i] > dsc[j] }) {
		t.Error("Slice was not sorted descending:", dsc)
		return
	}
}

func TestHeadTail(t *testing.T) {
	results := doQueryTest(test_setup.RandomRecord(2, 10, 1000), t, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query().Head(3), tbl.Query().Tail(4)}
	})
	defer results[0].Release()
	defer results[1].Release()

	if results[0].NumRows() != 3 {
		t.Error("Head returned wrong size")
		return
	}

	if results[1].NumRows() != 4 {
		t.Error("Tail returned wrong size")
		return
	}
}

func TestSelectDistinct(t *testing.T) {
	results := doQueryTest(test_setup.RandomRecord(2, 20, 10), t, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query().SelectDistinct("a")}
	})
	defer results[0].Release()

	if results[0].NumCols() != 1 || results[0].NumRows() > 10 {
		t.Errorf("SelectDistinct had wrong size %d x %d", results[0].NumCols(), results[0].NumRows())
		return
	}
}

func TestComboAgg(t *testing.T) {
	results := doQueryTest(test_setup.RandomRecord(3, 20, 10), t, func(tbl *client.TableHandle) []client.QueryNode {
		b := client.NewAggBuilder().Min("minB = b").Sum("sumC = c")
		return []client.QueryNode{tbl.Query().AggBy(b, "a")}
	})
	defer results[0].Release()

	if results[0].NumCols() != 3 || results[0].NumRows() > 10 {
		t.Errorf("ComboAgg had wrong size %d x %d", results[0].NumCols(), results[0].NumRows())
		return
	}
}

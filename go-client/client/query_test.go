package client_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_tools"
)

// execQueryOrSerial can be either (*client.Client).ExecQuery or (*client.Client).ExecSerial.
type execQueryOrSerial func(*client.Client, context.Context, ...client.QueryNode) ([]*client.TableHandle, error)

func TestDagQueryBatched(t *testing.T) {
	dagQuery(t, (*client.Client).ExecQuery)
}

func TestDagQuerySerial(t *testing.T) {
	dagQuery(t, (*client.Client).ExecSerial)
}

func dagQuery(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_tools.ExampleRecord()
	defer rec.Release()

	// Close (float32), Vol (int32), Ticker (string)
	exTable, err := c.ImportTable(ctx, rec)
	test_tools.CheckError(t, "ImportTable", err)
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
	finalQuery := client.MergeQuery("", otherQuery, exCloseLenQuery)

	tables, err := exec(c, ctx, finalQuery, otherQuery, exCloseLenQuery, exLenQuery)
	test_tools.CheckError(t, "ExecQuery", err)
	if len(tables) != 4 {
		t.Errorf("wrong number of tables")
		return
	}

	finalTable, err := tables[0].Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	otherTable, err := tables[1].Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	exCloseLenTable, err := tables[2].Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	exLenTable, err := tables[3].Snapshot(ctx)
	test_tools.CheckError(t, "Snapsnot", err)

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

	for _, tbl := range tables {
		err = tbl.Release(ctx)
		test_tools.CheckError(t, "Release", err)
	}
}

func TestMergeQueryBatched(t *testing.T) {
	mergeQuery(t, (*client.Client).ExecQuery)
}

func TestMergeQuerySerial(t *testing.T) {
	mergeQuery(t, (*client.Client).ExecSerial)
}

func mergeQuery(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	left, err := c.EmptyTable(ctx, 10)
	test_tools.CheckError(t, "EmptyTable", err)
	defer left.Release(ctx)

	right, err := c.EmptyTable(ctx, 5)
	test_tools.CheckError(t, "EmptyTable", err)
	defer right.Release(ctx)

	tables, err := exec(c, ctx, client.MergeQuery("", left.Query(), right.Query()))
	test_tools.CheckError(t, "ExecQuery", err)
	if len(tables) != 1 {
		t.Errorf("wrong number of tables")
	}

	tbl, err := tables[0].Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)

	if tbl.NumRows() != 15 || tbl.NumCols() != 0 {
		t.Errorf("table was wrong size")
	}

	err = tables[0].Release(ctx)
	test_tools.CheckError(t, "Release", err)
}

func TestEmptyMergeBatched(t *testing.T) {
	emptyMerge(t, (*client.Client).ExecQuery)
}

func TestEmptyMergeSerial(t *testing.T) {
	emptyMerge(t, (*client.Client).ExecSerial)
}

func emptyMerge(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	invalidMerge := client.MergeQuery("")

	_, err = exec(c, ctx, invalidMerge)
	if err == nil {
		t.Error("empty merge did not return error")
	}
}

func TestSeparateQueriesBatched(t *testing.T) {
	separateQueries(t, (*client.Client).ExecQuery)
}

func TestSeparateQueriesSerial(t *testing.T) {
	separateQueries(t, (*client.Client).ExecSerial)
}

func separateQueries(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	left := c.EmptyTableQuery(123)
	right := c.TimeTableQuery(10000000, time.Now())

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

func TestEmptyTableQueryBatched(t *testing.T) {
	emptyTableQuery(t, (*client.Client).ExecQuery)
}

func TestEmptyTableQuerySerial(t *testing.T) {
	emptyTableQuery(t, (*client.Client).ExecSerial)
}

func emptyTableQuery(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	base := c.EmptyTableQuery(123)

	derived := base.Update("a = ii")

	tables, err := exec(c, ctx, base, derived)
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

	for _, tbl := range tables {
		err = tbl.Release(ctx)
		test_tools.CheckError(t, "Release", err)
	}
}

func TestUpdateDropQueryBatched(t *testing.T) {
	updateDropQuery(t, (*client.Client).ExecQuery)
}

func TestUpdateDropQuerySerial(t *testing.T) {
	updateDropQuery(t, (*client.Client).ExecSerial)
}

func updateDropQuery(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	input := test_tools.ExampleRecord()
	defer input.Release()

	before, err := c.ImportTable(ctx, input)
	if err != nil {
		t.Errorf("ImportTable %s", err.Error())
		return
	}
	defer before.Release(ctx)

	updateQuery := before.Query().Update("Foo = Close * 17.0", "Bar = Vol + 1")
	dropQuery := updateQuery.DropColumns("Bar", "Ticker")

	tables, err := exec(c, ctx, updateQuery, dropQuery)
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

	for _, tbl := range tables {
		err = tbl.Release(ctx)
		test_tools.CheckError(t, "Release", err)
	}
}

func TestDuplicateQueryBatched(t *testing.T) {
	duplicateQuery(t, (*client.Client).ExecQuery)
}

func TestDuplicateQuerySerial(t *testing.T) {
	duplicateQuery(t, (*client.Client).ExecSerial)
}

func duplicateQuery(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
		return
	}
	defer c.Close()

	query1 := c.EmptyTableQuery(10).Update("b = ii + 2")
	query2 := query1.Update("a = ii * 3")

	tables, err := exec(c, ctx, query2, query1, query2, query1)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return
	}

	var records []arrow.Record
	for _, tbl := range tables {
		rec, err := tbl.Snapshot(ctx)
		if err != nil {
			t.Errorf("Snapshot %s", err.Error())
			return
		}
		defer rec.Release()

		records = append(records, rec)
	}

	if records[0].NumCols() != 2 || records[2].NumCols() != 2 {
		t.Errorf("query2 had wrong size")
	}

	if records[1].NumCols() != 1 || records[3].NumCols() != 1 {
		t.Errorf("query1 had wrong size")
	}

	for _, tbl := range tables {
		err = tbl.Release(ctx)
		test_tools.CheckError(t, "Release", err)
	}
}

func TestInvalidTableQueryBatched(t *testing.T) {
	invalidTableQuery(t, (*client.Client).ExecQuery)
}

func TestInvalidTableQuerySerial(t *testing.T) {
	invalidTableQuery(t, (*client.Client).ExecSerial)
}

func invalidTableQuery(t *testing.T, exec execQueryOrSerial) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
		return
	}
	defer c.Close()

	tbl := &client.TableHandle{}

	node1 := tbl.Query()
	node2 := node1.Update("a = ii * 3")

	_, err = exec(c, ctx, node1)
	if !errors.Is(err, client.ErrInvalidTableHandle) {
		t.Errorf("query on invalid table returned wrong error %s", err)
		return
	}

	_, err = exec(c, ctx, node2)
	if !errors.Is(err, client.ErrInvalidTableHandle) {
		t.Errorf("query on invalid table returned wrong error %s", err)
		return
	}
}

type queryOp func(*client.TableHandle) []client.QueryNode

func doQueryTest(inputRec arrow.Record, t *testing.T, exec execQueryOrSerial, op queryOp) []arrow.Record {
	defer inputRec.Release()

	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
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

	tables, err := exec(c, ctx, query...)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return nil
	}

	var recs []arrow.Record
	for _, table := range tables {
		rec, err := table.Snapshot(ctx)
		if err != nil {
			t.Errorf("Snapshot %s", err.Error())
			return nil
		}
		recs = append(recs, rec)
		err = table.Release(ctx)
		if err != nil {
			t.Errorf("Release %s", err.Error())
			return nil
		}
	}

	return recs
}

func TestEmptyUpdateQueryBatched(t *testing.T) {
	emptyUpdateQuery(t, (*client.Client).ExecQuery)
}

func TestEmptyUpdateQuerySerial(t *testing.T) {
	emptyUpdateQuery(t, (*client.Client).ExecSerial)
}

func emptyUpdateQuery(t *testing.T, exec execQueryOrSerial) {
	result := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query().Update()}
	})
	defer result[0].Release()
}

func TestEmptyQueryBatched(t *testing.T) {
	emptyQuery(t, (*client.Client).ExecQuery)
}

func TestEmptyQuerySerial(t *testing.T) {
	emptyQuery(t, (*client.Client).ExecSerial)
}

func emptyQuery(t *testing.T, exec execQueryOrSerial) {
	doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{}
	})
}

func TestNoopQueryBatched(t *testing.T) {
	noopQuery(t, (*client.Client).ExecQuery)
}

func TestNoopQuerySerial(t *testing.T) {
	noopQuery(t, (*client.Client).ExecSerial)
}

func noopQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query()}
	})
	defer results[0].Release()

	if results[0].NumCols() != 2 || results[0].NumRows() != 30 {
		t.Errorf("result had wrong size")
	}
}

func TestSortQueryBatched(t *testing.T) {
	sortQuery(t, (*client.Client).ExecQuery)
}

func TestSortQuerySerial(t *testing.T) {
	sortQuery(t, (*client.Client).ExecSerial)
}

func sortQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 10, 1000), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
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

func TestHeadTailQueryBatched(t *testing.T) {
	headTailQuery(t, (*client.Client).ExecQuery)
}

func TestHeadTailQuerySerial(t *testing.T) {
	headTailQuery(t, (*client.Client).ExecSerial)
}

func headTailQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 10, 1000), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
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

func TestSelectDistinctQueryBatched(t *testing.T) {
	selectDistinctQuery(t, (*client.Client).ExecQuery)
}

func TestSelectDistinctQuerySerial(t *testing.T) {
	selectDistinctQuery(t, (*client.Client).ExecSerial)
}

func selectDistinctQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 20, 10), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query().SelectDistinct("a")}
	})
	defer results[0].Release()

	if results[0].NumCols() != 1 || results[0].NumRows() > 10 {
		t.Errorf("SelectDistinct had wrong size %d x %d", results[0].NumCols(), results[0].NumRows())
		return
	}
}

func TestComboAggQueryBatched(t *testing.T) {
	comboAggQuery(t, (*client.Client).ExecQuery)
}

func TestComboAggQuerySerial(t *testing.T) {
	comboAggQuery(t, (*client.Client).ExecSerial)
}

func comboAggQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(4, 20, 10), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		b := client.NewAggBuilder().Min("minB = b").Sum("sumC = c")
		return []client.QueryNode{tbl.Query().AggBy(b, "a")}
	})
	defer results[0].Release()

	if results[0].NumCols() != 3 || results[0].NumRows() > 10 {
		t.Errorf("ComboAgg had wrong size %d x %d", results[0].NumCols(), results[0].NumRows())
		return
	}
}

func TestWhereQueryBatched(t *testing.T) {
	whereQuery(t, (*client.Client).ExecQuery)
}

func TestWhereQuerySerial(t *testing.T) {
	whereQuery(t, (*client.Client).ExecSerial)
}

func whereQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.ExampleRecord(), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query().Where("Vol % 1000 != 0")}
	})
	defer results[0].Release()

	if results[0].NumCols() != 3 || results[0].NumRows() != 3 {
		t.Error("Where had wrong size")
		return
	}
}

func TestUpdateViewSelectQueryBatched(t *testing.T) {
	updateViewSelectQuery(t, (*client.Client).ExecQuery)
}

func TestUpdateViewSelectQuerySerial(t *testing.T) {
	updateViewSelectQuery(t, (*client.Client).ExecSerial)
}

func updateViewSelectQuery(t *testing.T, exec execQueryOrSerial) {
	type usvOp func(qb client.QueryNode, columns ...string) client.QueryNode

	ops := []usvOp{client.QueryNode.Update, client.QueryNode.LazyUpdate, client.QueryNode.View, client.QueryNode.UpdateView, client.QueryNode.Select}

	for _, op := range ops {
		results := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
			return []client.QueryNode{op(tbl.Query(), "Sum = a + b", "b", "Foo = Sum % 2")}
		})
		defer results[0].Release()

		if results[0].NumCols() < 3 || results[0].NumRows() != 30 {
			t.Errorf("result had wrong size %d x %d", results[0].NumCols(), results[0].NumRows())
			return
		}
	}
}

func TestExactJoinQueryBatched(t *testing.T) {
	exactJoinQuery(t, (*client.Client).ExecQuery)
}

func TestExactJoinQuerySerial(t *testing.T) {
	exactJoinQuery(t, (*client.Client).ExecSerial)
}

func exactJoinQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(5, 100, 50), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		query := tbl.Query().GroupBy("a").Update("b = b[0]", "c = c[0]", "d = d[0]", "e = e[0]") // Make sure the key column is only unique values
		leftTable := query.DropColumns("c", "d", "e")
		rightTable := query.DropColumns("b", "c")
		resultTable := leftTable.ExactJoin(rightTable, []string{"a"}, []string{"d", "e"})
		return []client.QueryNode{leftTable, resultTable}
	})
	defer results[0].Release()
	defer results[1].Release()

	leftTable, resultTable := results[0], results[1]
	if resultTable.NumCols() != 4 || resultTable.NumRows() != leftTable.NumRows() {
		t.Errorf("result table had wrong size %d x %d", resultTable.NumCols(), resultTable.NumRows())
		return
	}
}

func TestNaturalJoinQueryBatched(t *testing.T) {
	naturalJoinQuery(t, (*client.Client).ExecQuery)
}

func TestNaturalJoinQuerySerial(t *testing.T) {
	naturalJoinQuery(t, (*client.Client).ExecSerial)
}

func naturalJoinQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(5, 100, 50), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		query := tbl.Query().GroupBy("a").Update("b = b[0]", "c = c[0]", "d = d[0]", "e = e[0]") // Make sure the key column is only unique values
		leftTable := query.DropColumns("c", "d", "e")
		rightTable := query.DropColumns("b", "c").Head(10)
		resultTable := leftTable.NaturalJoin(rightTable, []string{"a"}, []string{"d", "e"})
		return []client.QueryNode{leftTable, resultTable}
	})
	defer results[0].Release()
	defer results[1].Release()

	leftTable, resultTable := results[0], results[1]
	if resultTable.NumCols() != 4 || resultTable.NumRows() != leftTable.NumRows() {
		t.Errorf("result table had wrong size %d x %d", resultTable.NumCols(), resultTable.NumRows())
		return
	}
}

func TestCrossJoinQueryBatched(t *testing.T) {
	crossJoinQuery(t, (*client.Client).ExecQuery)
}

func TestCrossJoinQuerySerial(t *testing.T) {
	crossJoinQuery(t, (*client.Client).ExecSerial)
}

func crossJoinQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(5, 100, 50), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		leftTable := tbl.Query().DropColumns("e")
		rightTable := tbl.Query().Where("a % 2 > 0 && b % 3 == 1").DropColumns("b", "c", "d")
		resultTbl1 := leftTable.Join(rightTable, []string{"a"}, []string{"e"}, 10)
		resultTbl2 := leftTable.Join(rightTable, nil, []string{"e"}, 10)
		return []client.QueryNode{leftTable, rightTable, resultTbl1, resultTbl2}
	})
	defer results[0].Release()
	defer results[1].Release()
	defer results[2].Release()
	defer results[3].Release()

	left, _, result1, result2 := results[0], results[1], results[2], results[3]

	if result1.NumRows() >= left.NumRows() {
		t.Error("result1 was too large")
		return
	}

	if result2.NumRows() <= left.NumRows() {
		t.Error("result2 was too small")
		return
	}
}

func TestAsOfJoinQuery(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	startTime := time.Now().Add(time.Duration(-2) * time.Second)

	tt1 := c.TimeTableQuery(100000, startTime).Update("Col1 = i")
	tt2 := c.TimeTableQuery(200000, startTime).Update("Col1 = i")

	normalTable := tt1.AsOfJoin(tt2, []string{"Col1", "Timestamp"}, nil, client.MatchRuleLessThanEqual)
	reverseTable := tt1.AsOfJoin(tt2, []string{"Col1", "Timestamp"}, nil, client.MatchRuleGreaterThanEqual)

	tables, err := c.ExecQuery(ctx, tt1, normalTable, reverseTable)
	if err != nil {
		t.Errorf("ExecQuery %s", err.Error())
		return
	}
	if len(tables) != 3 {
		t.Errorf("wrong number of tables")
		return
	}
	defer tables[0].Release(ctx)
	defer tables[1].Release(ctx)
	defer tables[2].Release(ctx)

	ttRec, err := tables[0].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	normalRec, err := tables[1].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	reverseRec, err := tables[2].Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return
	}

	if normalRec.NumRows() == 0 || normalRec.NumRows() > ttRec.NumRows() {
		t.Error("record had wrong size")
		return
	}

	if reverseRec.NumRows() == 0 || reverseRec.NumRows() > ttRec.NumRows() {
		t.Error("record had wrong size")
		return
	}
}

func TestHeadByTailByQueryBatched(t *testing.T) {
	headByTailByQuery(t, (*client.Client).ExecQuery)
}

func TestHeadByTailByQuerySerial(t *testing.T) {
	headByTailByQuery(t, (*client.Client).ExecSerial)
}

func headByTailByQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(3, 10, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		query := tbl.Query()
		headTbl := query.HeadBy(1, "a")
		tailTbl := query.TailBy(1, "b")
		return []client.QueryNode{headTbl, tailTbl}
	})
	defer results[0].Release()
	defer results[1].Release()

	headTbl, tailTbl := results[0], results[1]
	if headTbl.NumRows() > 5 {
		t.Errorf("head table had wrong size %d", headTbl.NumRows())
		return
	}
	if tailTbl.NumRows() > 5 {
		t.Errorf("tail table had wrong size %d", tailTbl.NumRows())
		return
	}
}

func TestGroupQueryBatched(t *testing.T) {
	groupQuery(t, (*client.Client).ExecQuery)
}

func TestGroupQuerySerial(t *testing.T) {
	groupQuery(t, (*client.Client).ExecSerial)
}

func groupQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		query := tbl.Query()
		oneCol := query.GroupBy("a")
		bothCols := query.GroupBy()
		return []client.QueryNode{oneCol, bothCols}
	})
	defer results[0].Release()
	defer results[1].Release()

	oneCol, bothCols := results[0], results[1]
	if oneCol.NumRows() > 5 {
		t.Errorf("one-column-grouped table had wrong size %d", oneCol.NumRows())
		return
	}
	if bothCols.NumRows() > 25 {
		t.Errorf("all-grouped table had wrong size %d", bothCols.NumRows())
		return
	}
}

func TestUngroupQueryBatched(t *testing.T) {
	ungroupQuery(t, (*client.Client).ExecQuery)
}

func TestUngroupQuerySerial(t *testing.T) {
	ungroupQuery(t, (*client.Client).ExecSerial)
}

func ungroupQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		ungrouped := tbl.Query().GroupBy("a").Ungroup([]string{"b"}, false)
		return []client.QueryNode{ungrouped}
	})
	defer results[0].Release()

	ungrouped := results[0]
	if ungrouped.NumRows() != 30 {
		t.Errorf("table had wrong size %d", ungrouped.NumRows())
		return
	}
}

func TestCountByQueryBatched(t *testing.T) {
	countByQuery(t, (*client.Client).ExecQuery)
}

func TestCountByQuerySerial(t *testing.T) {
	countByQuery(t, (*client.Client).ExecSerial)
}

func countByQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		query := tbl.Query()
		distinct := query.SelectDistinct("a")
		counted := query.CountBy("Counted", "a")
		return []client.QueryNode{distinct, counted}
	})
	defer results[0].Release()

	distinct, counted := results[0], results[1]

	if distinct.NumRows() != counted.NumRows() || counted.NumCols() != 2 {
		t.Errorf("table had wrong size %d x %d (expected %d by %d)", counted.NumCols(), counted.NumRows(), 3, distinct.NumRows())
	}
}

func TestCountQueryBatched(t *testing.T) {
	countQuery(t, (*client.Client).ExecQuery)
}

func TestCountQuerySerial(t *testing.T) {
	countQuery(t, (*client.Client).ExecSerial)
}

func countQuery(t *testing.T, exec execQueryOrSerial) {
	results := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
		return []client.QueryNode{tbl.Query().Count("a")}
	})
	defer results[0].Release()

	result := results[0].Column(0).(*array.Int64).Int64Values()[0]
	if result != 30 {
		t.Errorf("Count returned wrong value %d", result)
		return
	}
}

func TestDedicatedAggQueryBatched(t *testing.T) {
	dedicatedAggQuery(t, (*client.Client).ExecQuery)
}

func TestDedicatedAggQuerySerial(t *testing.T) {
	dedicatedAggQuery(t, (*client.Client).ExecSerial)
}

func dedicatedAggQuery(t *testing.T, exec execQueryOrSerial) {
	type AggOp = func(qb client.QueryNode, by ...string) client.QueryNode

	ops := []AggOp{
		client.QueryNode.FirstBy, client.QueryNode.LastBy, client.QueryNode.SumBy, client.QueryNode.AvgBy, client.QueryNode.StdBy,
		client.QueryNode.VarBy, client.QueryNode.MedianBy, client.QueryNode.MinBy, client.QueryNode.MaxBy, client.QueryNode.AbsSumBy}

	for _, op := range ops {
		results := doQueryTest(test_tools.RandomRecord(2, 30, 5), t, exec, func(tbl *client.TableHandle) []client.QueryNode {
			return []client.QueryNode{op(tbl.Query(), "a")}
		})
		defer results[0].Release()

		if results[0].NumRows() > 5 {
			t.Errorf("table had wrong size %d", results[0].NumRows())
			return
		}
	}
}

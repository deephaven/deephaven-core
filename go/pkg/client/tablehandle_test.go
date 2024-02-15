package client_test

import (
	"context"
	"errors"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/deephaven/deephaven-core/go/internal/test_tools"
	"github.com/deephaven/deephaven-core/go/pkg/client"
	"slices"
	"sort"
	"testing"
)

type unaryTableOp func(context.Context, *client.TableHandle) (*client.TableHandle, error)

func applyTableOp(input arrow.Record, t *testing.T, op unaryTableOp) arrow.Record {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	before, err := c.ImportTable(ctx, input)
	if err != nil {
		t.Errorf("ImportTable %s", err.Error())
		return nil
	}
	defer before.Release(ctx)

	after, err := op(ctx, before)
	if err != nil {
		t.Errorf("table operation %s", err.Error())
		return nil
	}
	defer after.Release(ctx)

	result, err := after.Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return nil
	}

	return result
}

func TestDropColumns(t *testing.T) {
	result := applyTableOp(test_tools.ExampleRecord(), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.DropColumns(ctx, "Ticker", "Volume")
	})
	defer result.Release()

	if result.NumCols() != 1 {
		t.Errorf("wrong number of columns %d", result.NumCols())
		return
	}

	if result.ColumnName(0) != "Close" {
		t.Errorf("wrong column name %s", result.ColumnName(0))
		return
	}
}

func TestUpdateViewSelect(t *testing.T) {
	type usvOp func(*client.TableHandle, context.Context, ...string) (*client.TableHandle, error)

	ops := []usvOp{
		(*client.TableHandle).Update, (*client.TableHandle).LazyUpdate, (*client.TableHandle).UpdateView,
		(*client.TableHandle).View, (*client.TableHandle).Select,
	}

	for _, op := range ops {
		result := applyTableOp(test_tools.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
			return op(before, ctx, "Sum = a + b", "b", "Foo = Sum % 2")
		})
		defer result.Release()

		if result.NumCols() < 3 || result.NumRows() != 30 {
			t.Errorf("wrong number of columns %d", result.NumCols())
			return
		}
	}
}

func TestSelectDistinct(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(2, 20, 10), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.SelectDistinct(ctx, "a")
	})
	defer result.Release()

	if result.NumCols() != 1 || result.NumRows() > 10 {
		t.Errorf("SelectDistinct had wrong size %d x %d", result.NumCols(), result.NumRows())
		return
	}
}

func TestWhere(t *testing.T) {
	result := applyTableOp(test_tools.ExampleRecord(), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Where(ctx, "Volume % 1000 != 0")
	})
	defer result.Release()

	if result.NumCols() != 3 || result.NumRows() != 3 {
		t.Errorf("Where had wrong size %d x %d", result.NumCols(), result.NumRows())
		return
	}
}

func TestSort(t *testing.T) {
	asc := applyTableOp(test_tools.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Sort(ctx, "a")
	})
	defer asc.Release()
	ascData := asc.Column(0).(*array.Int32).Int32Values()
	if !sort.SliceIsSorted(ascData, func(i, j int) bool { return ascData[i] < ascData[j] }) {
		t.Error("Slice was not sorted ascending: ", asc)
		return
	}

	dsc := applyTableOp(test_tools.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.SortBy(ctx, client.SortDsc("a"))
	})
	defer dsc.Release()
	dscData := dsc.Column(0).(*array.Int32).Int32Values()
	if !sort.SliceIsSorted(dscData, func(i, j int) bool { return dscData[i] > dscData[j] }) {
		t.Error("Slice was not sorted descnding: ", dsc)
		return
	}
}

func TestHeadTail(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Head(ctx, 3)
	})
	if result.NumRows() != 3 {
		t.Errorf("head had wrong size %d", result.NumRows())
		return
	}
	result.Release()

	result = applyTableOp(test_tools.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Tail(ctx, 6)
	})
	if result.NumRows() != 6 {
		t.Errorf("tail had wrong size %d", result.NumRows())
	}
	result.Release()
}

func TestComboAgg(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(4, 30, 10), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		b := client.NewAggBuilder().Min("minB = b").Sum("sumC = c")
		return before.AggBy(ctx, b, "a")
	})
	defer result.Release()

	if result.NumCols() != 3 || result.NumRows() > 10 {
		t.Errorf("AggBy had wrong size %d x %d", result.NumCols(), result.NumRows())
		return
	}
}

func TestMerge(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(2, 30, 10), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		head, err := before.Head(ctx, 5)
		if err != nil {
			return nil, err
		}
		defer head.Release(ctx)

		return client.Merge(ctx, "a", before, head)
	})

	if result.NumRows() != 35 {
		t.Errorf("Merge had wrong size")
		return
	}
}

func TestMergeNull(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(2, 30, 10), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		head, err := before.Head(ctx, 5)
		if err != nil {
			return nil, err
		}
		defer head.Release(ctx)

		return client.Merge(ctx, "a", before, head, nil)
	})

	if result.NumRows() != 35 {
		t.Errorf("Merge had wrong size")
		return
	}
}

func TestEmptyMerge(t *testing.T) {
	ctx := context.Background()

	_, err := client.Merge(ctx, "", nil)
	if !errors.Is(err, client.ErrEmptyMerge) {
		t.Error("empty Merge returned wrong or missing error", err)
		return
	}
}

func TestExactJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_tools.RandomRecord(5, 100, 50)
	defer rec.Release()

	recTbl, err := c.ImportTable(ctx, rec)
	test_tools.CheckError(t, "ImportTable", err)
	defer recTbl.Release(ctx)

	tmp1, err := recTbl.GroupBy(ctx, "a")
	test_tools.CheckError(t, "GroupBy", err)
	defer tmp1.Release(ctx)

	base, err := tmp1.Update(ctx, "b = b[0]", "c = c[0]", "d = d[0]", "e = e[0]")
	test_tools.CheckError(t, "Update", err)
	defer base.Release(ctx)

	leftTbl, err := base.DropColumns(ctx, "c", "d", "e")
	test_tools.CheckError(t, "DropColumns", err)
	defer leftTbl.Release(ctx)

	rightTbl, err := base.DropColumns(ctx, "b", "c")
	test_tools.CheckError(t, "DropColumns", err)
	defer rightTbl.Release(ctx)

	resultTbl, err := leftTbl.ExactJoin(ctx, rightTbl, []string{"a"}, []string{"d", "e"})
	test_tools.CheckError(t, "ExactJoin", err)
	defer resultTbl.Release(ctx)

	leftRec, err := leftTbl.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer leftRec.Release()

	resultRec, err := resultTbl.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer resultRec.Release()

	if resultRec.NumCols() != 4 || resultRec.NumRows() != leftRec.NumRows() {
		t.Errorf("result table had wrong size %d x %d", resultRec.NumCols(), resultRec.NumRows())
		return
	}
}

func TestNaturalJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_tools.RandomRecord(5, 100, 50)
	defer rec.Release()

	recTbl, err := c.ImportTable(ctx, rec)
	test_tools.CheckError(t, "ImportTable", err)
	defer recTbl.Release(ctx)

	tmp1, err := recTbl.GroupBy(ctx, "a")
	test_tools.CheckError(t, "GroupBy", err)
	defer tmp1.Release(ctx)

	base, err := tmp1.Update(ctx, "b = b[0]", "c = c[0]", "d = d[0]", "e = e[0]")
	test_tools.CheckError(t, "Update", err)
	defer base.Release(ctx)

	leftTbl, err := base.DropColumns(ctx, "c", "d", "e")
	test_tools.CheckError(t, "DropColumns", err)
	defer leftTbl.Release(ctx)

	rightTbl, err := base.DropColumns(ctx, "b", "c")
	test_tools.CheckError(t, "DropColumns", err)
	defer rightTbl.Release(ctx)

	resultTbl, err := leftTbl.NaturalJoin(ctx, rightTbl, []string{"a"}, []string{"d", "e"})
	test_tools.CheckError(t, "NaturalJoin", err)
	defer resultTbl.Release(ctx)

	leftRec, err := leftTbl.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer leftRec.Release()

	resultRec, err := resultTbl.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer resultRec.Release()

	if resultRec.NumCols() != 4 || resultRec.NumRows() != leftRec.NumRows() {
		t.Errorf("result table had wrong size %d x %d", resultRec.NumCols(), resultRec.NumRows())
		return
	}
}

func TestCrossJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_tools.RandomRecord(5, 100, 50)
	defer rec.Release()

	recTbl, err := c.ImportTable(ctx, rec)
	test_tools.CheckError(t, "ImportTable", err)
	defer recTbl.Release(ctx)

	leftTbl, err := recTbl.DropColumns(ctx, "e")
	test_tools.CheckError(t, "DropColumns", err)
	defer leftTbl.Release(ctx)

	tmp1, err := recTbl.Where(ctx, "a % 2 > 0 && b % 3 == 1")
	test_tools.CheckError(t, "Where", err)
	defer tmp1.Release(ctx)

	rightTbl, err := tmp1.DropColumns(ctx, "b", "c", "d")
	test_tools.CheckError(t, "DropColumns", err)
	defer rightTbl.Release(ctx)

	resultTbl1, err := leftTbl.Join(ctx, rightTbl, []string{"a"}, []string{"e"}, 10)
	test_tools.CheckError(t, "Join", err)
	defer resultTbl1.Release(ctx)

	resultTbl2, err := leftTbl.Join(ctx, rightTbl, nil, []string{"e"}, 10)
	test_tools.CheckError(t, "Join", err)
	defer resultTbl2.Release(ctx)

	leftRec, err := leftTbl.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer leftRec.Release()

	rightRec, err := rightTbl.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer rightRec.Release()

	resultRec1, err := resultTbl1.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer resultRec1.Release()

	resultRec2, err := resultTbl2.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer resultRec2.Release()

	if resultRec1.NumRows() == 0 {
		t.Error("resultRec1 is empty")
		return
	}

	if resultRec1.NumRows() >= leftRec.NumRows()*rightRec.NumRows() {
		t.Errorf("resultRec1 is the wrong size: %v >= %v", resultRec1.NumRows(), leftRec.NumRows()*rightRec.NumRows())
		return
	}

	if resultRec2.NumRows() != leftRec.NumRows()*rightRec.NumRows() {
		t.Errorf("resultRec2 is the wrong size: %v != %v", resultRec2.NumRows(), leftRec.NumRows()*rightRec.NumRows())
		return
	}
}

func TestAsOfJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	tempty, err := c.EmptyTable(ctx, 5)
	test_tools.CheckError(t, "EmptyTable", err)
	defer tempty.Release(ctx)

	tleft, err := tempty.Update(ctx, "Time = i * 3", "LValue = 100 + i")
	test_tools.CheckError(t, "Update", err)
	defer tleft.Release(ctx)

	tright, err := tempty.Update(ctx, "Time = i * 5", "RValue = 200 + i")
	test_tools.CheckError(t, "Update", err)
	defer tright.Release(ctx)

	taojLeq, err := tleft.AsOfJoin(ctx, tright, []string{"Time"}, nil, client.MatchRuleLessThanEqual)
	test_tools.CheckError(t, "AsOfJoin", err)
	defer taojLeq.Release(ctx)

	taojGeq, err := tleft.AsOfJoin(ctx, tright, []string{"Time"}, nil, client.MatchRuleGreaterThanEqual)
	test_tools.CheckError(t, "AsOfJoin", err)
	defer taojGeq.Release(ctx)

	leqRec, err := taojLeq.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer leqRec.Release()

	geqRec, err := taojGeq.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer geqRec.Release()

	// Column 2 is the RValue column
	actualLeqData := leqRec.Column(2).(*array.Int32).Int32Values()
	actualGeqData := geqRec.Column(2).(*array.Int32).Int32Values()

	expectedLeqData := []int32{200, 200, 201, 201, 202}
	expectedGeqData := []int32{200, 201, 202, 202, 203}

	if !slices.Equal(expectedLeqData, actualLeqData) {
		t.Errorf("leq values different expected %v != actual %v", expectedLeqData, actualLeqData)
	}

	if !slices.Equal(expectedGeqData, actualGeqData) {
		t.Errorf("geq values different: expected %v != actual %v", expectedGeqData, actualGeqData)
	}
}

func TestHeadBy(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(3, 10, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.HeadBy(ctx, 1, "a")
	})
	if result.NumRows() > 5 {
		t.Error("result had too many rows")
		return
	}
	result.Release()
}

func TestTailBy(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(3, 10, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.TailBy(ctx, 1, "a")
	})
	if result.NumRows() > 5 {
		t.Errorf("result had too many rows")
	}
	result.Release()
}

func TestGroupBy(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.GroupBy(ctx, "a")
	})
	if result.NumRows() > 5 {
		t.Errorf("one-column-grouped table had wrong size %d", result.NumRows())
		return
	}
	result.Release()

	result = applyTableOp(test_tools.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.GroupBy(ctx, "a")
	})
	if result.NumRows() > 25 {
		t.Errorf("all-grouped table had wrong size %d", result.NumRows())
		return
	}
	result.Release()
}

func TestUngroup(t *testing.T) {
	result := applyTableOp(test_tools.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		tbl, err := before.GroupBy(ctx, "a")
		if err != nil {
			return nil, err
		}
		defer tbl.Release(ctx)
		return tbl.Ungroup(ctx, []string{"b"}, false)
	})
	if result.NumRows() != 30 {
		t.Errorf("ungrouped table had wrong size %d", result.NumRows())
		return
	}
	result.Release()
}

func TestCountBy(t *testing.T) {
	rec := test_tools.RandomRecord(2, 30, 5)
	rec.Retain()

	distinct := applyTableOp(rec, t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.SelectDistinct(ctx, "a")
	})
	defer distinct.Release()
	numDistinct := distinct.NumRows()

	counted := applyTableOp(rec, t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.CountBy(ctx, "Counted", "a")
	})
	defer counted.Release()

	if counted.NumRows() != numDistinct {
		t.Errorf("counted had wrong size")
		return
	}
}

func TestCount(t *testing.T) {
	count := applyTableOp(test_tools.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Count(ctx, "b")
	})
	defer count.Release()

	result := count.Column(0).(*array.Int64).Int64Values()[0]
	if result != 30 {
		t.Errorf("count was incorrect: %d", result)
		return
	}
}

func TestDedicatedAgg(t *testing.T) {
	type aggOp = func(*client.TableHandle, context.Context, ...string) (*client.TableHandle, error)

	ops := []aggOp{
		(*client.TableHandle).FirstBy, (*client.TableHandle).LastBy, (*client.TableHandle).SumBy, (*client.TableHandle).AvgBy,
		(*client.TableHandle).StdBy, (*client.TableHandle).VarBy, (*client.TableHandle).MedianBy, (*client.TableHandle).MinBy,
		(*client.TableHandle).MaxBy, (*client.TableHandle).AbsSumBy,
	}

	for _, op := range ops {
		result := applyTableOp(test_tools.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
			return op(before, ctx, "a")
		})
		defer result.Release()

		if result.NumRows() > 5 {
			t.Errorf("table had wrong size %d", result.NumRows())
			return
		}
	}
}

func TestZeroTable(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	tbl1 := &client.TableHandle{}
	tbl2, err := tbl1.Update(ctx, "foo = i")
	if err == nil || !errors.Is(err, client.ErrInvalidTableHandle) {
		t.Error("wrong error for updating zero table:", err.Error())
		return
	}
	if tbl2 != nil {
		t.Errorf("returned table was not nil")
		return
	}

	err = tbl1.Release(ctx)
	if err != nil {
		t.Error("error when releasing zero table", err.Error())
	}
}

func TestReleasedTable(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	test_tools.CheckError(t, "NewClient", err)
	defer c.Close()

	tbl1, err := c.EmptyTable(ctx, 10)
	test_tools.CheckError(t, "EmptyTable", err)

	err = tbl1.Release(ctx)
	test_tools.CheckError(t, "Release", err)

	tbl2, err := tbl1.Update(ctx, "foo = i")
	if err == nil || !errors.Is(err, client.ErrInvalidTableHandle) {
		t.Error("wrong error for updating released table:", err.Error())
		return
	}
	if tbl2 != nil {
		t.Errorf("returned table was not nil")
		return
	}

	err = tbl1.Release(ctx)
	if err != nil {
		t.Error("error when releasing released table", err.Error())
	}
}

func TestDifferentClients(t *testing.T) {
	ctx := context.Background()

	client1, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	test_tools.CheckError(t, "NewClient", err)
	defer client1.Close()

	client2, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	test_tools.CheckError(t, "NewClient", err)
	defer client2.Close()

	table1, err := client1.EmptyTable(ctx, 5)
	test_tools.CheckError(t, "EmptyTable", err)
	defer table1.Release(ctx)

	table2, err := client2.EmptyTable(ctx, 5)
	test_tools.CheckError(t, "EmptyTable", err)
	defer table2.Release(ctx)

	type makeTableOp func() (*client.TableHandle, error)

	crossJoin := func() (*client.TableHandle, error) {
		return table1.Join(ctx, table2, nil, nil, 10)
	}
	exactJoin := func() (*client.TableHandle, error) {
		return table1.ExactJoin(ctx, table2, nil, nil)
	}
	naturalJoin := func() (*client.TableHandle, error) {
		return table1.NaturalJoin(ctx, table2, nil, nil)
	}
	asOfJoin := func() (*client.TableHandle, error) {
		return table1.AsOfJoin(ctx, table2, nil, nil, client.MatchRuleLessThanEqual)
	}
	merge := func() (*client.TableHandle, error) {
		return client.Merge(ctx, "", table1, table2)
	}

	ops := []makeTableOp{crossJoin, exactJoin, naturalJoin, asOfJoin, merge}
	for _, op := range ops {
		_, err := op()
		if !errors.Is(err, client.ErrDifferentClients) {
			t.Errorf("missing or incorrect error %s", err)
			return
		}
	}
}

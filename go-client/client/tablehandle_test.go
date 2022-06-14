package client_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_setup"
)

type unaryTableOp func(context.Context, *client.TableHandle) (*client.TableHandle, error)

func applyTableOp(input arrow.Record, t *testing.T, op unaryTableOp) arrow.Record {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_setup.GetHost(), test_setup.GetPort(), "python")
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
		t.Errorf("DropColumns %s", err.Error())
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
	result := applyTableOp(test_setup.ExampleRecord(), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.DropColumns(ctx, "Ticker", "Vol")
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
		result := applyTableOp(test_setup.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
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
	result := applyTableOp(test_setup.RandomRecord(2, 20, 10), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.SelectDistinct(ctx, "a")
	})
	defer result.Release()

	if result.NumCols() != 1 || result.NumRows() > 10 {
		t.Errorf("SelectDistinct had wrong size %d x %d", result.NumCols(), result.NumRows())
		return
	}
}

func TestWhere(t *testing.T) {
	result := applyTableOp(test_setup.ExampleRecord(), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Where(ctx, "Vol % 1000 != 0")
	})
	defer result.Release()

	if result.NumCols() != 3 || result.NumRows() != 3 {
		t.Errorf("Where had wrong size %d x %d", result.NumCols(), result.NumRows())
		return
	}
}

func TestSort(t *testing.T) {
	asc := applyTableOp(test_setup.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Sort(ctx, "a")
	})
	defer asc.Release()
	ascData := asc.Column(0).(*array.Int32).Int32Values()
	if !sort.SliceIsSorted(ascData, func(i, j int) bool { return ascData[i] < ascData[j] }) {
		t.Error("Slice was not sorted ascending: ", asc)
		return
	}

	dsc := applyTableOp(test_setup.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
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
	result := applyTableOp(test_setup.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Head(ctx, 3)
	})
	if result.NumRows() != 3 {
		t.Errorf("head had wrong size %d", result.NumRows())
		return
	}
	result.Release()

	result = applyTableOp(test_setup.RandomRecord(2, 10, 1000), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.Tail(ctx, 6)
	})
	if result.NumRows() != 6 {
		t.Errorf("tail had wrong size %d", result.NumRows())
	}
	result.Release()
}

func TestComboAgg(t *testing.T) {
	result := applyTableOp(test_setup.RandomRecord(4, 30, 10), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
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
	result := applyTableOp(test_setup.RandomRecord(2, 30, 10), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		head, err := before.Head(ctx, 5)
		if err != nil {
			return nil, err
		}
		defer head.Release(ctx)

		return before.Merge(ctx, "a", head)
	})

	if result.NumRows() != 35 {
		t.Errorf("Merge had wrong size")
		return
	}
}

func TestExactJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_setup.GetHost(), test_setup.GetPort(), "python")
	test_setup.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_setup.RandomRecord(5, 100, 50)
	defer rec.Release()

	recTbl, err := c.ImportTable(ctx, rec)
	test_setup.CheckError(t, "ImportTable", err)
	defer recTbl.Release(ctx)

	tmp1, err := recTbl.GroupBy(ctx, "a")
	test_setup.CheckError(t, "GroupBy", err)
	defer tmp1.Release(ctx)

	base, err := tmp1.Update(ctx, "b = b[0]", "c = c[0]", "d = d[0]", "e = e[0]")
	test_setup.CheckError(t, "Update", err)
	defer base.Release(ctx)

	leftTbl, err := base.DropColumns(ctx, "c", "d", "e")
	test_setup.CheckError(t, "DropColumns", err)
	defer leftTbl.Release(ctx)

	rightTbl, err := base.DropColumns(ctx, "b", "c")
	test_setup.CheckError(t, "DropColumns", err)
	defer rightTbl.Release(ctx)

	resultTbl, err := leftTbl.ExactJoin(ctx, rightTbl, []string{"a"}, []string{"d", "e"})
	test_setup.CheckError(t, "ExactJoin", err)
	defer resultTbl.Release(ctx)

	leftRec, err := leftTbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer leftRec.Release()

	resultRec, err := resultTbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer resultRec.Release()

	if resultRec.NumCols() != 4 || resultRec.NumRows() != leftRec.NumRows() {
		t.Errorf("result table had wrong size %d x %d", resultRec.NumCols(), resultRec.NumRows())
		return
	}
}

func TestNaturalJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_setup.GetHost(), test_setup.GetPort(), "python")
	test_setup.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_setup.RandomRecord(5, 100, 50)
	defer rec.Release()

	recTbl, err := c.ImportTable(ctx, rec)
	test_setup.CheckError(t, "ImportTable", err)
	defer recTbl.Release(ctx)

	tmp1, err := recTbl.GroupBy(ctx, "a")
	test_setup.CheckError(t, "GroupBy", err)
	defer tmp1.Release(ctx)

	base, err := tmp1.Update(ctx, "b = b[0]", "c = c[0]", "d = d[0]", "e = e[0]")
	test_setup.CheckError(t, "Update", err)
	defer base.Release(ctx)

	leftTbl, err := base.DropColumns(ctx, "c", "d", "e")
	test_setup.CheckError(t, "DropColumns", err)
	defer leftTbl.Release(ctx)

	rightTbl, err := base.DropColumns(ctx, "b", "c")
	test_setup.CheckError(t, "DropColumns", err)
	defer rightTbl.Release(ctx)

	resultTbl, err := leftTbl.NaturalJoin(ctx, rightTbl, []string{"a"}, []string{"d", "e"})
	test_setup.CheckError(t, "NaturalJoin", err)
	defer resultTbl.Release(ctx)

	leftRec, err := leftTbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer leftRec.Release()

	resultRec, err := resultTbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer resultRec.Release()

	if resultRec.NumCols() != 4 || resultRec.NumRows() != leftRec.NumRows() {
		t.Errorf("result table had wrong size %d x %d", resultRec.NumCols(), resultRec.NumRows())
		return
	}
}

func TestCrossJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_setup.GetHost(), test_setup.GetPort(), "python")
	test_setup.CheckError(t, "NewClient", err)
	defer c.Close()

	rec := test_setup.RandomRecord(5, 100, 50)
	defer rec.Release()

	recTbl, err := c.ImportTable(ctx, rec)
	test_setup.CheckError(t, "ImportTable", err)
	defer recTbl.Release(ctx)

	leftTbl, err := recTbl.DropColumns(ctx, "e")
	test_setup.CheckError(t, "DropColumns", err)
	defer leftTbl.Release(ctx)

	tmp1, err := recTbl.Where(ctx, "a % 2 > 0 && b % 3 == 1")
	test_setup.CheckError(t, "Where", err)
	defer tmp1.Release(ctx)

	rightTbl, err := tmp1.DropColumns(ctx, "b", "c", "d")
	test_setup.CheckError(t, "DropColumns", err)
	defer rightTbl.Release(ctx)

	resultTbl1, err := leftTbl.Join(ctx, rightTbl, []string{"a"}, []string{"e"}, 10)
	test_setup.CheckError(t, "Join", err)
	defer resultTbl1.Release(ctx)

	resultTbl2, err := leftTbl.Join(ctx, rightTbl, nil, []string{"e"}, 10)
	test_setup.CheckError(t, "Join", err)
	defer resultTbl2.Release(ctx)

	leftRec, err := leftTbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer leftRec.Release()

	resultRec1, err := resultTbl1.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer resultRec1.Release()

	resultRec2, err := resultTbl2.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer resultRec2.Release()

	if resultRec1.NumRows() >= leftRec.NumRows() {
		t.Error("resultRec1 was too large")
		return
	}

	if resultRec2.NumRows() <= leftRec.NumRows() {
		t.Error("resultRec2 was too small")
		return
	}
}

func TestAsOfJoin(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_setup.GetHost(), test_setup.GetPort(), "python")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	startTime := time.Now().UnixNano() - 2_000_000_000

	tmp1, err := c.TimeTable(ctx, 100000, &startTime)
	test_setup.CheckError(t, "TimeTable", err)
	defer tmp1.Release(ctx)

	tt1, err := tmp1.Update(ctx, "Col1 = i")
	test_setup.CheckError(t, "Update", err)
	defer tt1.Release(ctx)

	tmp2, err := c.TimeTable(ctx, 200000, &startTime)
	test_setup.CheckError(t, "TimeTable", err)
	defer tmp2.Release(ctx)

	tt2, err := tmp2.Update(ctx, "Col1 = i")
	test_setup.CheckError(t, "Update", err)
	defer tt2.Release(ctx)

	normalTbl, err := tt1.AsOfJoin(ctx, tt2, []string{"Col1", "Timestamp"}, nil, client.MatchRuleLessThanEqual)
	test_setup.CheckError(t, "AsOfJoin", err)
	defer normalTbl.Release(ctx)

	reverseTbl, err := tt1.AsOfJoin(ctx, tt2, []string{"Col1", "Timestamp"}, nil, client.MatchRuleGreaterThanEqual)
	test_setup.CheckError(t, "AsOfJoin", err)
	defer reverseTbl.Release(ctx)

	ttRec, err := tt1.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer ttRec.Release()

	normalRec, err := normalTbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer normalRec.Release()

	reverseRec, err := reverseTbl.Snapshot(ctx)
	test_setup.CheckError(t, "Snapshot", err)
	defer reverseRec.Release()

	if normalRec.NumRows() == 0 || normalRec.NumRows() > ttRec.NumRows() {
		t.Error("record had wrong size")
		return
	}

	if reverseRec.NumRows() == 0 || reverseRec.NumRows() > ttRec.NumRows() {
		t.Error("record had wrong size")
		return
	}
}

func TestHeadByTailBy(t *testing.T) {
	result := applyTableOp(test_setup.RandomRecord(3, 10, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.HeadBy(ctx, 1, "a")
	})
	if result.NumRows() > 5 {
		t.Error("result had too many rows")
		return
	}
	result.Release()

	result = applyTableOp(test_setup.RandomRecord(3, 10, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.TailBy(ctx, 1, "a")
	})
	if result.NumRows() > 5 {
		t.Errorf("result had too many rows")
	}
	result.Release()
}

func TestGroup(t *testing.T) {
	result := applyTableOp(test_setup.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.GroupBy(ctx, "a")
	})
	if result.NumRows() > 5 {
		t.Errorf("one-column-grouped table had wrong size %d", result.NumRows())
		return
	}
	result.Release()

	result = applyTableOp(test_setup.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
		return before.GroupBy(ctx, "a")
	})
	if result.NumRows() > 25 {
		t.Errorf("all-grouped table had wrong size %d", result.NumRows())
		return
	}
	result.Release()
}

func TestUngroup(t *testing.T) {
	result := applyTableOp(test_setup.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
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
	rec := test_setup.RandomRecord(2, 30, 5)
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
	count := applyTableOp(test_setup.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
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
		result := applyTableOp(test_setup.RandomRecord(2, 30, 5), t, func(ctx context.Context, before *client.TableHandle) (*client.TableHandle, error) {
			return op(before, ctx, "a")
		})
		defer result.Release()

		if result.NumRows() > 5 {
			t.Errorf("table had wrong size %d", result.NumRows())
			return
		}
	}
}

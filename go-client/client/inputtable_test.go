package client_test

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_tools"
)

// getDataTableSchema returns the schema for an example record.
func getDataTableSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Vol", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
}

// getDataTableFirstRow imports and returns the first row of an example record.
func getDataTableFirstRow(ctx context.Context, client *client.Client) (*client.TableHandle, error) {
	rec := test_tools.ExampleRecord()
	defer rec.Release()

	tbl, err := client.ImportTable(ctx, rec)
	if err != nil {
		return nil, err
	}
	defer tbl.Release(ctx)

	firstPart, err := tbl.Head(ctx, 1)
	if err != nil {
		return nil, err
	}

	return firstPart, err
}

// getDataTableSecondPart imports and returns the first five rows of an example record.
// This intentionally overlaps with getDataTableSecondPart.
func getDataTableFirstPart(ctx context.Context, client *client.Client) (*client.TableHandle, error) {
	rec := test_tools.ExampleRecord()
	defer rec.Release()

	tbl, err := client.ImportTable(ctx, rec)
	if err != nil {
		return nil, err
	}
	defer tbl.Release(ctx)

	firstPart, err := tbl.Head(ctx, 5)
	if err != nil {
		return nil, err
	}

	return firstPart, err
}

// getDataTableSecondPart imports and returns the last five rows of an example record.
// This intentionally overlaps with getDataTableFirstPart.
func getDataTableSecondPart(ctx context.Context, client *client.Client) (*client.TableHandle, error) {
	rec := test_tools.ExampleRecord()
	defer rec.Release()

	tbl, err := client.ImportTable(ctx, rec)
	if err != nil {
		return nil, err
	}
	defer tbl.Release(ctx)

	secondPart, err := tbl.Tail(ctx, 5)
	if err != nil {
		return nil, err
	}

	return secondPart, err
}

// addNewDataToAppend gets two (overlapping) parts of an example table and
// adds them to the provided input table. It returns records from
// a filtered version of the input table.
// It returns three records from the filtered table:
// - A snapshot before any data has been added
// - A snapshot once the first part of the data has been added
// - A snapshot once both parts of the data has been added.
// The parts overlap to check that an input table allows duplicate rows,
// and the filtered table is used to check that changes to the input table
// propogate to other tables.
func addNewDataToAppend(
	ctx context.Context, cl *client.Client, tbl *client.AppendOnlyInputTable,
) (before arrow.Record, mid arrow.Record, after arrow.Record, err error) {
	newData1, err := getDataTableFirstPart(ctx, cl)
	if err != nil {
		return
	}
	defer newData1.Release(ctx)
	newData2, err := getDataTableSecondPart(ctx, cl)
	if err != nil {
		return
	}
	defer newData2.Release(ctx)

	output, err := tbl.Where(ctx, "Close > 30.0")
	if err != nil {
		return
	}

	before, err = output.Snapshot(ctx)
	if err != nil {
		return
	}

	err = tbl.AddTable(ctx, newData1)
	if err != nil {
		return
	}

	mid, err = output.Snapshot(ctx)
	if err != nil {
		return
	}

	err = tbl.AddTable(ctx, newData2)
	if err != nil {
		return
	}

	after, err = output.Snapshot(ctx)

	return
}

func TestAppendOnlyFromSchema(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	inputTable, err := cl.NewAppendOnlyInputTableFromSchema(ctx, getDataTableSchema())
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inputTable.Release(ctx)

	before, mid, after, err := addNewDataToAppend(ctx, cl, inputTable)
	test_tools.CheckError(t, "addNewDataToAppend", err)
	defer before.Release()
	defer mid.Release()
	defer after.Release()

	if before.NumCols() != 3 || before.NumRows() != 0 {
		t.Errorf("before had wrong size %d x %d", before.NumCols(), before.NumRows())
	}

	if mid.NumCols() != 3 || mid.NumRows() != 4 {
		t.Errorf("mid had wrong size %d x %d", mid.NumCols(), mid.NumRows())
	}

	if after.NumCols() != 3 || after.NumRows() != 7 {
		t.Errorf("after had wrong size %d x %d", after.NumCols(), after.NumRows())
	}
}

func TestAppendOnlyFromTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	templateTbl, err := getDataTableFirstRow(ctx, cl)
	test_tools.CheckError(t, "GetDataTableFirstRow", err)
	defer templateTbl.Release(ctx)

	inputTable, err := cl.NewAppendOnlyInputTableFromTable(ctx, templateTbl)
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inputTable.Release(ctx)

	before, mid, after, err := addNewDataToAppend(ctx, cl, inputTable)
	test_tools.CheckError(t, "addNewDataToAppend", err)
	defer before.Release()
	defer mid.Release()
	defer after.Release()

	if before.NumCols() != 3 || before.NumRows() != 0 {
		t.Errorf("before had wrong size %d x %d", before.NumCols(), before.NumRows())
	}

	if mid.NumCols() != 3 || mid.NumRows() != 4 {
		t.Errorf("mid had wrong size %d x %d", mid.NumCols(), mid.NumRows())
	}

	if after.NumCols() != 3 || after.NumRows() != 7 {
		t.Errorf("after had wrong size %d x %d", after.NumCols(), after.NumRows())
	}
}

func TestKeyBackedTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	inputTable, err := cl.NewKeyBackedInputTableFromSchema(ctx, getDataTableSchema(), "Ticker")
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inputTable.Release(ctx)

	delData2, err := getDataTableFirstRow(ctx, cl)
	test_tools.CheckError(t, "GetDataTableFirstRow", err)
	defer delData2.Release(ctx)
	delData, err := delData2.View(ctx, "Ticker")
	test_tools.CheckError(t, "View", err)
	defer delData.Release(ctx)

	newData1, err := getDataTableFirstPart(ctx, cl)
	test_tools.CheckError(t, "GetDataTableFirstPart", err)
	defer newData1.Release(ctx)
	newData2, err := getDataTableSecondPart(ctx, cl)
	test_tools.CheckError(t, "GetDataTableSecondPart", err)
	defer newData2.Release(ctx)

	outputTable, err := inputTable.Where(ctx, "Close > 30.0")
	test_tools.CheckError(t, "Where", err)
	defer outputTable.Release(ctx)

	err = inputTable.AddTable(ctx, newData1)
	test_tools.CheckError(t, "AddTable", err)

	mid1, err := outputTable.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer mid1.Release()

	err = inputTable.AddTable(ctx, newData2)
	test_tools.CheckError(t, "AddTable", err)

	mid2, err := outputTable.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer mid2.Release()

	err = inputTable.DeleteTable(ctx, delData)
	test_tools.CheckError(t, "DeleteTable", err)

	after, err := outputTable.Snapshot(ctx)
	test_tools.CheckError(t, "Snapshot", err)
	defer after.Release()

	if mid1.NumCols() != 3 || mid1.NumRows() != 4 {
		t.Errorf("mid1 had wrong size %d x %d", mid1.NumCols(), mid1.NumRows())
	}

	if mid2.NumCols() != 3 || mid2.NumRows() != 5 {
		t.Errorf("mid2 had wrong size %d x %d", mid2.NumCols(), mid2.NumRows())
	}

	if after.NumCols() != 3 || after.NumRows() != 4 {
		t.Errorf("after had wrong size %d x %d", after.NumCols(), after.NumRows())
	}
}

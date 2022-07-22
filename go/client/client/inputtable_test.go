package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/deephaven/deephaven-core/go/client/client"
	"github.com/deephaven/deephaven-core/go/client/internal/test_tools"
)

// getDataTableSchema returns the schema for an example record.
func getDataTableSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Volume", Type: arrow.PrimitiveTypes.Int32},
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

// A checkerFunc returns nil if the given Record matches what it expects, otherwise it returns some error.
type checkerFunc func(record arrow.Record) error

// checkTable repeatedly snapshots a table and checks it using the provided function.
// If the table still fails the check after the timeout has expired, this returns an error.
func checkTable(ctx context.Context, table *client.TableHandle, checker checkerFunc, timeout time.Duration) error {
	timeoutChannel := time.After(timeout)

	var lastError error

	for {
		select {
		case <-timeoutChannel:
			return lastError
		default:
			record, snapshotErr := table.Snapshot(ctx)
			if snapshotErr != nil {
				return snapshotErr
			}
			lastError = checker(record)
			record.Release()
			if lastError == nil {
				return nil
			}
		}
	}
}

// addNewDataToAppend gets two (overlapping) parts of an example table and
// adds them to the provided input table. It returns records from
// a filtered version of the input table.
// It will check three records from the filtered table:
// - A snapshot before any data has been added
// - A snapshot once the first part of the data has been added
// - A snapshot after both parts of the data has been added.
// The parts overlap to check that an input table allows duplicate rows,
// and the filtered table is used to check that changes to the input table
// propogate to other tables.
func addNewDataToAppend(
	ctx context.Context, cl *client.Client, tbl *client.AppendOnlyInputTable,
	beforeCheck checkerFunc, midCheck checkerFunc, afterCheck checkerFunc,
) (err error) {
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

	err = checkTable(ctx, output, beforeCheck, time.Second*5)
	if err != nil {
		return
	}

	err = tbl.AddTable(ctx, newData1)
	if err != nil {
		return
	}

	err = checkTable(ctx, output, midCheck, time.Second*5)
	if err != nil {
		return
	}

	err = tbl.AddTable(ctx, newData2)
	if err != nil {
		return
	}

	err = checkTable(ctx, output, afterCheck, time.Second*5)
	if err != nil {
		return
	}

	err = output.Release(ctx)

	return
}

func TestAppendOnlyFromSchema(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	inputTable, err := cl.NewAppendOnlyInputTableFromSchema(ctx, getDataTableSchema())
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inputTable.Release(ctx)

	beforeCheck := func(before arrow.Record) error {
		if before.NumCols() != 3 || before.NumRows() != 0 {
			return fmt.Errorf("before had wrong size %d x %d", before.NumCols(), before.NumRows())
		}
		return nil
	}

	midCheck := func(mid arrow.Record) error {
		if mid.NumCols() != 3 || mid.NumRows() != 4 {
			return fmt.Errorf("mid had wrong size %d x %d", mid.NumCols(), mid.NumRows())
		}
		return nil
	}

	afterCheck := func(after arrow.Record) error {
		if after.NumCols() != 3 || after.NumRows() != 7 {
			return fmt.Errorf("after had wrong size %d x %d", after.NumCols(), after.NumRows())
		}
		return nil
	}

	err = addNewDataToAppend(ctx, cl, inputTable, beforeCheck, midCheck, afterCheck)
	test_tools.CheckError(t, "addNewDataToAppend", err)
}

func TestAppendOnlyFromTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	templateTbl, err := getDataTableFirstRow(ctx, cl)
	test_tools.CheckError(t, "GetDataTableFirstRow", err)
	defer templateTbl.Release(ctx)

	inputTable, err := cl.NewAppendOnlyInputTableFromTable(ctx, templateTbl)
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inputTable.Release(ctx)

	beforeCheck := func(before arrow.Record) error {
		if before.NumCols() != 3 || before.NumRows() != 0 {
			return fmt.Errorf("before had wrong size %d x %d", before.NumCols(), before.NumRows())
		}
		return nil
	}

	midCheck := func(mid arrow.Record) error {
		if mid.NumCols() != 3 || mid.NumRows() != 4 {
			return fmt.Errorf("mid had wrong size %d x %d", mid.NumCols(), mid.NumRows())
		}
		return nil
	}

	afterCheck := func(after arrow.Record) error {
		if after.NumCols() != 3 || after.NumRows() != 7 {
			return fmt.Errorf("after had wrong size %d x %d", after.NumCols(), after.NumRows())
		}
		return nil
	}

	err = addNewDataToAppend(ctx, cl, inputTable, beforeCheck, midCheck, afterCheck)
	test_tools.CheckError(t, "addNewDataToAppend", err)
}

func TestKeyBackedTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
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

	mid1Check := func(mid1 arrow.Record) error {
		if mid1.NumCols() != 3 || mid1.NumRows() != 4 {
			return fmt.Errorf("mid1 had wrong size %d x %d", mid1.NumCols(), mid1.NumRows())
		}
		return nil
	}
	err = checkTable(ctx, outputTable, mid1Check, time.Second*5)
	test_tools.CheckError(t, "checkTable", err)

	err = inputTable.AddTable(ctx, newData2)
	test_tools.CheckError(t, "AddTable", err)

	mid2Check := func(mid2 arrow.Record) error {
		if mid2.NumCols() != 3 || mid2.NumRows() != 5 {
			return fmt.Errorf("mid2 had wrong size %d x %d", mid2.NumCols(), mid2.NumRows())
		}
		return nil
	}
	err = checkTable(ctx, outputTable, mid2Check, time.Second*5)
	test_tools.CheckError(t, "checkTable", err)

	err = inputTable.DeleteTable(ctx, delData)
	test_tools.CheckError(t, "DeleteTable", err)

	afterCheck := func(after arrow.Record) error {
		if after.NumCols() != 3 || after.NumRows() != 4 {
			return fmt.Errorf("after had wrong size %d x %d", after.NumCols(), after.NumRows())
		}
		return nil
	}
	err = checkTable(ctx, outputTable, afterCheck, time.Second*5)
	test_tools.CheckError(t, "checktable", err)
}

func TestInvalidInputTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	inTable := client.AppendOnlyInputTable{}

	_, err = inTable.Head(ctx, 50)
	if !errors.Is(err, client.ErrInvalidTableHandle) {
		t.Error("wrong or missing error", err)
		return
	}
}

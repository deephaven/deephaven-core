package ticking_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/deephaven/deephaven-core/go/internal/test_tools"
	"github.com/deephaven/deephaven-core/go/pkg/client"
	"github.com/deephaven/deephaven-core/go/pkg/client/ticking"
)

func sampleSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "K", Type: arrow.PrimitiveTypes.Int32},
			{Name: "V1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "V2", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
}

func randomSampleRecord(numRows int64) arrow.Record {
	schema := sampleSchema()

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for colIdx := 0; colIdx < len(schema.Fields()); colIdx++ {
		column := make([]int32, numRows)
		for i := 0; i < len(column); i++ {
			column[i] = int32(rand.Uint32())
		}
		b.Field(colIdx).(*array.Int32Builder).AppendValues(column, nil)
	}

	return b.NewRecord()
}

func addRecordAppend(ctx context.Context, cl *client.Client, inTable *client.AppendOnlyInputTable, record arrow.Record) error {
	dataTbl, err := cl.ImportTable(ctx, record)
	if err != nil {
		return err
	}

	err = inTable.AddTable(ctx, dataTbl)
	if err != nil {
		return err
	}

	err = dataTbl.Release(ctx)
	if err != nil {
		return err
	}

	return nil
}

const defaultTimeout = time.Duration(time.Second * 5)

func waitForUpdate(updateCh <-chan ticking.TickingStatus) (ticking.TickingUpdate, error) {
	select {
	case status, ok := <-updateCh:
		if !ok {
			return ticking.TickingUpdate{}, errors.New("channel closed")
		}
		if status.Err != nil {
			return ticking.TickingUpdate{}, status.Err
		}
		return status.Update, nil
	case <-time.After(defaultTimeout):
		return ticking.TickingUpdate{}, errors.New("timed out waiting for update")
	}
}

func waitForClose(updateCh <-chan ticking.TickingStatus) error {
	select {
	case _, ok := <-updateCh:
		if ok {
			return errors.New("got a response but the channel should be closed")
		} else {
			return nil
		}
	case <-time.After(defaultTimeout):
		return errors.New("timed out waiting for channel to close")
	}
}

// checkTable repeatedly snapshots a table and checks it using the provided function.
// If the table still fails the check after the timeout has expired, this returns an error.
func checkSnapshot(ctx context.Context, table *client.TableHandle, expected arrow.Record, timeout time.Duration) error {
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
			lastError = compareRecords(record, expected)
			record.Release()
			if lastError == nil {
				return nil
			}
		}
	}
}

func compareRecords(actual arrow.Record, expected arrow.Record) error {
	if actual.NumCols() != expected.NumCols() {
		return fmt.Errorf("differing number of columns (actual %d, expected %d)", actual.NumCols(), expected.NumCols())
	}

	if actual.NumRows() != expected.NumRows() {
		return fmt.Errorf("differing number of rows (actual %d, expected %d)", actual.NumRows(), expected.NumRows())
	}

	for colIdx := 0; colIdx < int(actual.NumCols()); colIdx++ {
		actualColumn := actual.Column(colIdx).(*array.Int32)
		expectedColumn := expected.Column(colIdx).(*array.Int32)

		for rowIdx := 0; rowIdx < int(actual.NumRows()); rowIdx++ {
			actualData := actualColumn.Int32Values()[rowIdx]
			expectedData := expectedColumn.Int32Values()[rowIdx]
			if actualData != expectedData {
				return fmt.Errorf("differing data (actual %d, expected %d)", actualData, expectedData)
			}
		}
	}

	return nil
}

func TestAppendingTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	schema := sampleSchema()

	inTable, err := cl.NewAppendOnlyInputTableFromSchema(ctx, schema)
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inTable.Release(ctx)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	tickingTbl, updateCh, err := inTable.Subscribe(subCtx)
	test_tools.CheckError(t, "Subscribe", err)

	update, err := waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	rec := randomSampleRecord(5)
	err = addRecordAppend(ctx, cl, inTable, rec)
	test_tools.CheckError(t, "addRecordAppend", err)
	defer rec.Release()

	update, err = waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	tickingData := tickingTbl.ToRecord()
	defer tickingData.Release()

	if err = compareRecords(tickingData, rec); err != nil {
		t.Log(tickingData)
		t.Log(rec)
		t.Error(err)
		return
	}

	subCancel()

	err = waitForClose(updateCh)
	if err != nil {
		t.Error(err)
		return
	}
}

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
			{Name: "KK", Type: arrow.PrimitiveTypes.Int32},
			{Name: "V1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "V2", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
}

func randomSampleRecord(numRows int64, maxValue uint32) arrow.Record {
	schema := sampleSchema()

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for colIdx := 0; colIdx < len(schema.Fields()); colIdx++ {
		column := make([]int32, numRows)
		for i := 0; i < len(column); i++ {
			column[i] = int32(rand.Uint32() % maxValue)
		}
		b.Field(colIdx).(*array.Int32Builder).AppendValues(column, nil)
	}

	return b.NewRecord()
}

// Creates a sample record, except it copies the keys from another record
func randomSampleRecordCopiedKeys(keyRecord arrow.Record, maxValue uint32) arrow.Record {
	schema := sampleSchema()

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues(keyRecord.Column(0).(*array.Int32).Int32Values(), nil)

	for colIdx := 1; colIdx < len(schema.Fields()); colIdx++ {
		column := make([]int32, keyRecord.NumRows())
		for i := 0; i < len(column); i++ {
			column[i] = int32(rand.Uint32() % maxValue)
		}
		b.Field(colIdx).(*array.Int32Builder).AppendValues(column, nil)
	}

	return b.NewRecord()
}

func getExpectedKeyTable(keyRecord arrow.Table) arrow.Record {
	getAllValues := func(column *arrow.Column) <-chan int32 {
		ch := make(chan int32)
		go func() {
			for _, chunk := range column.Data().Chunks() {
				data := chunk.(*array.Int32)
				for _, value := range data.Int32Values() {
					ch <- value
				}
			}
			close(ch)
		}()
		return ch
	}

	c0 := getAllValues(keyRecord.Column(0))
	c1 := getAllValues(keyRecord.Column(1))
	c2 := getAllValues(keyRecord.Column(2))

	var d0 []int32
	var d1 []int32
	var d2 []int32

	for k := range c0 {
		v1 := <-c1
		v2 := <-c2

		var keyFound bool

		for foundIdx, d := range d0 {
			if d == k {
				// Key already exists.
				keyFound = true
				d1[foundIdx] = v1
				d2[foundIdx] = v2
				break
			}
		}

		if !keyFound {
			// New key, so append it
			d0 = append(d0, k)
			d1 = append(d1, v1)
			d2 = append(d2, v2)
		}
	}

	schema := sampleSchema()

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues(d0, nil)
	b.Field(1).(*array.Int32Builder).AppendValues(d1, nil)
	b.Field(2).(*array.Int32Builder).AppendValues(d2, nil)

	return b.NewRecord()
}

func addRecordAppend(ctx context.Context, cl *client.Client, inTable client.AddableInputTable, record arrow.Record) error {
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
/*
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
*/

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

	rec := randomSampleRecord(5, 100)
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

func TestRemovingTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	schema := sampleSchema()

	inTable, err := cl.NewAppendOnlyInputTableFromSchema(ctx, schema)
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inTable.Release(ctx)

	outTable, err := inTable.Tail(ctx, 5)
	test_tools.CheckError(t, "Tail", err)
	defer outTable.Release(ctx)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	tickingTbl, updateCh, err := outTable.Subscribe(subCtx)
	test_tools.CheckError(t, "Subscribe", err)

	update, err := waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	rec1 := randomSampleRecord(5, 100)
	err = addRecordAppend(ctx, cl, inTable, rec1)
	test_tools.CheckError(t, "addRecordAppend", err)
	defer rec1.Release()

	update, err = waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	tickingData1 := tickingTbl.ToRecord()
	defer tickingData1.Release()

	if err = compareRecords(tickingData1, rec1); err != nil {
		t.Log(tickingData1)
		t.Log(rec1)
		t.Error(err)
		return
	}

	rec2 := randomSampleRecord(5, 100)
	err = addRecordAppend(ctx, cl, inTable, rec2)
	test_tools.CheckError(t, "addRecordAppend", err)
	defer rec2.Release()

	update, err = waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	tickingData2 := tickingTbl.ToRecord()
	defer tickingData2.Release()

	if err = compareRecords(tickingData2, rec2); err != nil {
		t.Log(tickingData2)
		t.Log(rec1)
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

func TestModifyingTable(t *testing.T) {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	test_tools.CheckError(t, "NewClient", err)
	defer cl.Close()

	schema := sampleSchema()

	inTable, err := cl.NewKeyBackedInputTableFromSchema(ctx, schema, "KK")
	test_tools.CheckError(t, "NewAppendOnlyInputTableFromSchema", err)
	defer inTable.Release(ctx)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	tickingTbl, updateCh, err := inTable.Subscribe(subCtx)
	test_tools.CheckError(t, "Subscribe", err)

	update, err := waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	rec1 := randomSampleRecord(20, 10)
	err = addRecordAppend(ctx, cl, inTable, rec1)
	test_tools.CheckError(t, "addRecordAppend", err)
	defer rec1.Release()

	update, err = waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	tickingData1 := tickingTbl.ToRecord()
	defer tickingData1.Release()

	expected1 := getExpectedKeyTable(array.NewTableFromRecords(rec1.Schema(), []arrow.Record{rec1}))

	if err = compareRecords(tickingData1, expected1); err != nil {
		t.Log(tickingData1)
		t.Log(rec1)
		t.Error(err)
		return
	}

	rec2 := randomSampleRecordCopiedKeys(rec1, 10)

	err = addRecordAppend(ctx, cl, inTable, rec2)
	test_tools.CheckError(t, "addRecordAppend", err)
	defer rec2.Release()

	update, err = waitForUpdate(updateCh)
	test_tools.CheckError(t, "waitForUpdate", err)
	tickingTbl.ApplyUpdate(update)

	tickingData2 := tickingTbl.ToRecord()
	defer tickingData2.Release()

	expected2 := getExpectedKeyTable(array.NewTableFromRecords(rec1.Schema(), []arrow.Record{rec1, rec2}))
	defer expected2.Release()

	if err = compareRecords(tickingData2, expected2); err != nil {
		t.Log(tickingData2)
		t.Log(expected2)
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

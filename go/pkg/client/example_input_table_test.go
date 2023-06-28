package client_test

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/deephaven/deephaven-core/go/internal/test_tools"
	"github.com/deephaven/deephaven-core/go/pkg/client"
)

// This example shows how to use Input Tables.
// Input Tables make are a generic interface for streaming data from any source,
// so you can use Deephaven's streaming table processing power for anything.
//
// This example requires a Deephaven server to connect to, so it will not work on pkg.go.dev.
func Example_inputTable() {
	// A context is used to set timeouts and deadlines for requests or cancel requests.
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), test_tools.GetAuthType(), test_tools.GetAuthToken())
	if err != nil {
		fmt.Println("error when connecting to server:", err.Error())
		return
	}
	defer cl.Close()

	// First, let's make a schema our input table is going to use.
	// This describes the name and data types each of its columns will have.
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Volume", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// Then we can actually make the input table.
	// It will start empty, but we're going to add more data to it.
	// This is a key-backed input table, so it will make sure the "Ticker" column stays unique.
	// This is in contrast to an append-only table, which will append rows to the end of the table.
	inputTable, err := cl.NewKeyBackedInputTableFromSchema(ctx, schema, "Ticker")
	if err != nil {
		fmt.Println("error when creating InputTable", err.Error())
		return
	}
	// Any tables you create should be eventually released.
	defer inputTable.Release(ctx)

	// Now let's create a table derived from the input table.
	// When we update the input table, this table will update too.
	outputTable, err := inputTable.Where(ctx, "Close > 50.0")
	if err != nil {
		fmt.Println("error when filtering input table", err.Error())
		return
	}
	defer outputTable.Release(ctx)

	// Now, let's get some new data to add to the input table.
	// We import the data so that it is available on the server.
	newDataRec := test_tools.ExampleRecord()
	// Note that Arrow records must be eventually released.
	defer newDataRec.Release()
	newDataTable, err := cl.ImportTable(ctx, newDataRec)
	if err != nil {
		fmt.Println("error when importing new data", err.Error())
		return
	}
	defer newDataTable.Release(ctx)

	// Now we can add the new data we just imported to our input table.
	// Since this is a key-backed table, it will add any rows with new keys
	// and replace any rows with keys that already exist.
	// Since there's currently nothing in the table,
	// this call will add all the rows of the new data to the input table.
	err = inputTable.AddTable(ctx, newDataTable)
	if err != nil {
		fmt.Println("error when adding new data to table", err.Error())
		return
	}

	// Changes made to an input table may not propogate to other tables immediately.
	// Thus, we need to check the output in a loop to see if our output table has updated.
	// In a future version of the API, streaming table updates will make this kind of check unnecessary.
	timeout := time.After(time.Second * 5)
	for {
		// If this loop is still running after five seconds,
		// it will terminate because of this timer.
		select {
		case <-timeout:
			fmt.Println("the output table did not update in time")
			return
		default:
		}

		// Now, we take a snapshot of the outputTable to see what data it currently contains.
		// We should see the new rows we added, filtered by the condition we specified when creating outputTable.
		// However, we might just see an empty table if the new rows haven't been processed yet.
		outputRec, err := outputTable.Snapshot(ctx)
		if err != nil {
			fmt.Println("error when snapshotting table", err.Error())
			return
		}

		if outputRec.NumRows() == 0 {
			// The new rows we added haven't propogated to the output table yet.
			// We just discard this record and snapshot again.
			outputRec.Release()
			continue
		}

		fmt.Println("Got the output table!")
		test_tools.RecordPrint(outputRec)
		outputRec.Release()
		break
	}

	// Output:
	// Got the output table!
	// record:
	//   schema:
	//   fields: 3
	//     - Ticker: type=utf8, nullable
	//         metadata: ["deephaven:inputtable.isKey": "true", "deephaven:isDateFormat": "false", "deephaven:isNumberFormat": "false", "deephaven:isPartitioning": "false", "deephaven:isRowStyle": "false", "deephaven:isSortable": "true", "deephaven:isStyle": "false", "deephaven:type": "java.lang.String"]
	//     - Close: type=float32, nullable
	//        metadata: ["deephaven:inputtable.isKey": "false", "deephaven:isDateFormat": "false", "deephaven:isNumberFormat": "false", "deephaven:isPartitioning": "false", "deephaven:isRowStyle": "false", "deephaven:isSortable": "true", "deephaven:isStyle": "false", "deephaven:type": "float"]
	//     - Volume: type=int32, nullable
	//         metadata: ["deephaven:inputtable.isKey": "false", "deephaven:isDateFormat": "false", "deephaven:isNumberFormat": "false", "deephaven:isPartitioning": "false", "deephaven:isRowStyle": "false", "deephaven:isSortable": "true", "deephaven:isStyle": "false", "deephaven:type": "int"]
	//   metadata: ["deephaven:unsent.attribute.InputTable": ""]
	//   rows: 4
	//   col[0][Ticker]: ["XRX" "XYZZY" "GME" "ZNGA"]
	//   col[1][Close]: [53.8 88.5 453 544.9]
	//   col[2][Volume]: [87000 6060842 138000000 48300]
}

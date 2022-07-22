package client_test

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go/client/client"
	"github.com/deephaven/deephaven-core/go/client/internal/test_tools"
)

// This example shows off the ability to upload tables to the Deephaven server,
// perform some operations on them,
// and then download them to access the modified data.
//
// This example requires a Deephaven server to connect to, so it will not work on pkg.go.dev.
func Example_importTable() {
	// A context is used to set timeouts and deadlines for requests or cancel requests.
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	if err != nil {
		fmt.Println("error when connecting to server:", err.Error())
		return
	}
	defer cl.Close()

	// First, we need some Arrow record we want to upload.
	sampleRecord := test_tools.ExampleRecord()
	// Note that Arrow records should be eventually released.
	defer sampleRecord.Release()

	fmt.Println("Data Before:")
	fmt.Println(sampleRecord)

	// Now we upload the record so that we can manipulate its data using the server.
	// We get back a TableHandle, which is a reference to a table on the server.
	table, err := cl.ImportTable(ctx, sampleRecord)
	if err != nil {
		fmt.Println("error when importing table:", err.Error())
		return
	}
	// Any tables you create should be eventually released.
	defer table.Release(ctx)

	// Now we can do a bunch of operations on the table we imported, if we like...

	// Note that table operations return new tables; they don't modify old tables.
	sortedTable, err := table.Sort(ctx, "Close")
	if err != nil {
		fmt.Println("error when sorting:", err.Error())
		return
	}
	defer sortedTable.Release(ctx)
	filteredTable, err := sortedTable.Where(ctx, "Volume >= 20000")
	if err != nil {
		fmt.Println("error when filtering:", err.Error())
		return
	}
	defer filteredTable.Release(ctx)

	// If we want to see the data we sorted and filtered, we can snapshot the table to get a Record back.
	filteredRecord, err := filteredTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when filtering:", err.Error())
		return
	}
	defer filteredRecord.Release()

	fmt.Println("Data After:")
	fmt.Println(filteredRecord)

	// Output:
	// Data Before:
	// record:
	//   schema:
	//   fields: 3
	//     - Ticker: type=utf8
	//     - Close: type=float32
	//     - Volume: type=int32
	//   rows: 7
	//   col[0][Ticker]: ["XRX" "XYZZY" "IBM" "GME" "AAPL" "ZNGA" "T"]
	//   col[1][Close]: [53.8 88.5 38.7 453 26.7 544.9 13.4]
	//   col[2][Volume]: [87000 6060842 138000 138000000 19000 48300 1500]
	//
	// Data After:
	// record:
	//   schema:
	//   fields: 3
	//     - Ticker: type=utf8, nullable
	//         metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "java.lang.String", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//     - Close: type=float32, nullable
	//        metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "float", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//     - Volume: type=int32, nullable
	//         metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "int", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//   metadata: ["deephaven:attribute.SortedColumns": "Close=Ascending", "deephaven:attribute_type.SortedColumns": "java.lang.String", "deephaven:attribute_type.AddOnly": "java.lang.Boolean", "deephaven:attribute.AddOnly": "true"]
	//   rows: 5
	//   col[0][Ticker]: ["IBM" "XRX" "XYZZY" "GME" "ZNGA"]
	//   col[1][Close]: [38.7 53.8 88.5 453 544.9]
	//   col[2][Volume]: [138000 87000 6060842 138000000 48300]
}

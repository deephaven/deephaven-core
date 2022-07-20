package client_test

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_tools"
)

// This example shows how to manipulate tables using the client.
//
// There are two different ways to manipulate tables: normal table operations, and the query system.
// See the doc comments for doQueryOps and doNormalOps for an explanation of each.
// Don't be afraid to mix and match both as the situation requires!
//
// This example requires a Deephaven server running on localhost:10000, so it will not work on pkg.go.dev.
func Example_tableOps() {
	normalResult, err := doNormalOps()
	if err != nil {
		fmt.Println("encountered an error:", err.Error())
		return
	}

	queryResult, err := doQueryOps()
	if err != nil {
		fmt.Println("encountered an error:", err.Error())
		return
	}

	if normalResult != queryResult {
		fmt.Println("results differed!", err.Error())
	}

	fmt.Println(queryResult)

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
	// New data:
	// record:
	//   schema:
	//   fields: 3
	//     - Ticker: type=utf8, nullable
	//         metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "java.lang.String", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//     - Close: type=float32, nullable
	//        metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "float", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//     - Volume: type=int32, nullable
	//         metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "int", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//   metadata: ["deephaven:attribute_type.AddOnly": "java.lang.Boolean", "deephaven:attribute.AddOnly": "true"]
	//   rows: 5
	//   col[0][Ticker]: ["XRX" "IBM" "GME" "AAPL" "ZNGA"]
	//   col[1][Close]: [53.8 38.7 453 26.7 544.9]
	//   col[2][Volume]: [87000 138000 138000000 19000 48300]
	//
	// record:
	//   schema:
	//   fields: 4
	//     - Ticker: type=utf8, nullable
	//         metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "java.lang.String", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//     - Close: type=float32, nullable
	//        metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "float", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//     - Volume: type=int32, nullable
	//         metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "int", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//     - Magnitude: type=int32, nullable
	//            metadata: ["deephaven:isRowStyle": "false", "deephaven:type": "int", "deephaven:isNumberFormat": "false", "deephaven:isStyle": "false", "deephaven:isRollupColumn": "false", "deephaven:isDateFormat": "false"]
	//   rows: 5
	//   col[0][Ticker]: ["XRX" "IBM" "GME" "AAPL" "ZNGA"]
	//   col[1][Close]: [53.8 38.7 453 26.7 544.9]
	//   col[2][Volume]: [87000 138000 138000000 19000 48300]
	//   col[3][Magnitude]: [10000 100000 100000000 10000 10000]
}

// This function demonstrates how to use normal table operations.
//
// Normal table operations take in TableHandles as inputs, and return a TableHandle (or an error) as an output.
// They allow for more fine-grained error handling and debugging than queries, at the cost of being more verbose.
func doNormalOps() (string, error) {
	// A context is used to set timeouts and deadlines for requests or cancel requests.
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	if err != nil {
		fmt.Println("error when connecting to server:", err.Error())
		return "", err
	}
	defer cl.Close()

	// First, let's create some example data to manipulate.
	sampleRecord := test_tools.ExampleRecord()
	// Note that Arrow records must eventually be released.
	defer sampleRecord.Release()

	// Now we upload the record as a table on the server.
	// We get back a TableHandle, which is a reference to a table on the server.
	baseTable, err := cl.ImportTable(ctx, sampleRecord)
	if err != nil {
		fmt.Println("error when uploading table:", err.Error())
		return "", err
	}
	// Table handles should be released when they are no longer needed.
	defer baseTable.Release(ctx)

	// Now, let's start doing table operations.
	// Maybe I don't like companies whose names are too long or too short, so let's keep only the ones in the middle.
	midStocks, err := baseTable.Where(ctx, "Ticker.length() == 3 || Ticker.length() == 4")
	if err != nil {
		fmt.Println("error when filtering table:", err.Error())
		return "", err
	}
	defer midStocks.Release(ctx)

	// We can also create completely new tables with the client too.
	// Let's make a table whose columns are powers of ten.
	powTenTable, err := cl.EmptyTable(ctx, 10)
	if err != nil {
		fmt.Println("error when creating an empty table:", err.Error())
		return "", err
	}
	defer powTenTable.Release(ctx)
	powTenTable, err = powTenTable.Update(ctx, "Magnitude = (int)pow(10, ii)")
	if err != nil {
		fmt.Println("error when updating a table:", err.Error())
		return "", err
	}
	defer powTenTable.Release(ctx)

	// What if I want to bin the companies according to the magnitude of the Volume column?
	// We can perform an as-of join between two tables to produce another table.
	magStocks, err := midStocks.
		AsOfJoin(ctx, powTenTable, []string{"Volume = Magnitude"}, nil, client.MatchRuleLessThanEqual)
	if err != nil {
		fmt.Println("error when doing an as-of join:", err.Error())
		return "", err
	}
	defer magStocks.Release(ctx)

	// Now, if we want to see the data in each of our tables, we can take snapshots.
	midRecord, err := midStocks.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting:", err.Error())
		return "", err
	}
	defer midRecord.Release()
	magRecord, err := magStocks.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting:", err.Error())
		return "", err
	}
	defer magRecord.Release()

	return fmt.Sprintf("Data Before:\n%s\nNew data:\n%s\n%s", sampleRecord, midRecord, magRecord), nil
}

// This function demonstrates how to use the query system.
//
// The query system allows you to build up an arbitrary number of table operations into a single object,
// and then execute all of the table operations at once.
// This simplifies error handling, is much more concise, and can be more efficient than doing the operations separately.
func doQueryOps() (string, error) {
	// A context is used to set timeouts and deadlines for requests or cancel requests.
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	if err != nil {
		fmt.Println("error when connecting to server:", err.Error())
		return "", err
	}
	defer cl.Close()

	// First, let's create some example data to manipulate.
	sampleRecord := test_tools.ExampleRecord()
	// Note that Arrow records must eventually be released.
	defer sampleRecord.Release()

	// Now we upload the record as a table on the server.
	// We get back a TableHandle, which is a reference to a table on the server.
	baseTable, err := cl.ImportTable(ctx, sampleRecord)
	if err != nil {
		fmt.Println("error when uploading table:", err.Error())
		return "", err
	}
	// Table handles should be released when they are no longer needed.
	defer baseTable.Release(ctx)

	// Now, let's start building a query.
	// Maybe I don't like companies whose names are too long or too short, so let's keep only the ones in the middle.
	midStocks := baseTable.Query().
		Where("Ticker.length() == 3 || Ticker.length() == 4")

	// We can use the query system to create completely new tables too.
	// Let's make a table whose columns are powers of ten.
	// Again, EmptyTableQuery() returns a QueryNode.
	powTenTable := cl.
		EmptyTableQuery(10).
		Update("Magnitude = (int)pow(10, ii)")

	// What if I want to bin the companies according to the magnitude of the Volume column?
	// Query methods can take other query nodes as arguments to build up arbitrarily complicated requests,
	// so we can perform an as-of join between two query nodes just fine.
	magStocks := midStocks.
		AsOfJoin(powTenTable, []string{"Volume = Magnitude"}, nil, client.MatchRuleLessThanEqual)

	// And now, we can execute the queries we have built.
	tables, err := cl.ExecBatch(ctx, midStocks, magStocks)
	if err != nil {
		fmt.Println("error when executing query:", err.Error())
		return "", err
	}
	// The order of the tables in the returned list is the same as the order of the queries passed as arguments.
	midTable, magTable := tables[0], tables[1]
	defer midTable.Release(ctx)
	defer magTable.Release(ctx)

	// Now, if we want to see the data in each of our tables, we can take snapshots.
	midRecord, err := midTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting:", err.Error())
		return "", err
	}
	defer midRecord.Release()
	magRecord, err := magTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting:", err.Error())
		return "", err
	}
	defer magRecord.Release()

	return fmt.Sprintf("Data Before:\n%s\nNew data:\n%s\n%s", sampleRecord, midRecord, magRecord), nil
}

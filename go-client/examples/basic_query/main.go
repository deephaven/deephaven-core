package main

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/examples/common"
)

// This example shows how to use the powerful query system,
// which allows you to perform an arbitrary number of table operations in a single request.
// It can even create new tables as part of a query,
// and the interface is more convenient than doing each of the table operations serially.
func main() {
	// A context is used to set timeouts and deadlines for requests or cancel requests.
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	// When starting a client connection, the client script language
	// must match the language the server was started with,
	// even if the client does not execute any scripts.
	cl, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}
	defer cl.Close()

	// First, let's create some example data to manipulate.
	sampleRecord := common.GetExampleRecord()
	// Note that Arrow records must eventually be released.
	defer sampleRecord.Release()

	fmt.Println("Data Before:")
	fmt.Println(sampleRecord)

	// Now we upload the record as a table on the server.
	// We get back a TableHandle, which is a reference to a table on the server.
	baseTable, err := cl.ImportTable(ctx, sampleRecord)
	if err != nil {
		fmt.Println("error when uploading table:", err.Error())
		return
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
	tables, err := cl.ExecQuery(ctx, midStocks, magStocks)
	if err != nil {
		fmt.Println("error when executing query:", err.Error())
		if e, ok := err.(client.QueryError); ok {
			fmt.Println(e.Details())
		}
		return
	}
	// The order of the tables in the returned list is the same as the order of the queries passed as arguments.
	midTable, magTable := tables[0], tables[1]
	defer midTable.Release(ctx)
	defer magTable.Release(ctx)

	// Now, if we want to see the data in each of our tables, we can take snapshots.
	midRecord, err := midTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting:", err.Error())
		return
	}
	defer midRecord.Release()
	magRecord, err := magTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting:", err.Error())
		return
	}
	defer magRecord.Release()

	// Queries are immensely powerful, easy to use, and highly efficient.
	// Use them!

	fmt.Println("New data!")
	fmt.Println(midRecord)
	fmt.Println(magRecord)
}

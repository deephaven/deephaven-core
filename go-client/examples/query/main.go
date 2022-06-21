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
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}
	defer cl.Close()

	// Let's get some example data to play with.
	sampleRecord := common.GetExampleRecord()
	defer sampleRecord.Release()

	fmt.Println("Data Before:")
	fmt.Println(sampleRecord)

	// We do need to upload the table data normally, without using a query.
	baseTable, err := cl.ImportTable(ctx, sampleRecord)
	if err != nil {
		fmt.Println("error when uploading table:", err.Error())
		return
	}
	defer baseTable.Release(ctx)

	// And now the fun begins.
	// Let's make a table whose columns are powers of ten.
	powTenTable := cl.
		EmptyTableQuery(10).
		Update("Magnitude = (int)pow(10, ii)")

	// Maybe I don't like companies whose names are too long or too short, so let's filter in only the ones in the middle.
	// Note that we use baseTable.Query() to start building a new query from an existing handle.
	midStocks := baseTable.Query().
		Where("Ticker.length() == 3 || Ticker.length() == 3")

	// What if I want to bin the companies according to the magnitude of the Vol column?
	// As-Of joins aren't just for timestamps--let's use them for this, too.
	// Note that we're combining completely unrelated tables and it will still make only a single query request!
	magStocks := midStocks.
		AsOfJoin(powTenTable, []string{"Vol = Magnitude"}, nil, client.MatchRuleLessThanEqual)

	// And now, we can execute all of these operations using only a single request.
	// Note that we can retrieve multiple tables from anywhere in the query.
	tables, err := cl.ExecQuery(ctx, midStocks, magStocks)
	if err != nil {
		fmt.Println("error when executing query:", err.Error())
		return
	}
	midTable, magTable := tables[0], tables[1]
	defer midTable.Release(ctx)
	defer magTable.Release(ctx)

	// Now, we can just snapshot our tables as normal if we want to see the data.

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

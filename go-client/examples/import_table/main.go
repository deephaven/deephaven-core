package main

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/examples/common"
)

// This example shows off the ability to upload tables to the Deephaven server,
// perform some operations on them,
// and then download them to access the modified data.
func main() {
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}
	defer cl.Close()

	// First, we need some Record we want to upload.
	sampleRecord := common.GetExampleRecord()
	defer sampleRecord.Release()

	fmt.Println("Data Before:")
	fmt.Println(sampleRecord)

	// Then, we can simply upload it to the Deephaven server.
	table, err := cl.ImportTable(ctx, sampleRecord)
	if err != nil {
		fmt.Println("error when importing table:", err.Error())
		return
	}
	defer table.Release(ctx)

	// Now we can do a bunch of operations on it, if we like...

	sortedTable, err := table.Sort(ctx, "Close")
	if err != nil {
		fmt.Println("error when sorting:", err.Error())
		return
	}
	defer sortedTable.Release(ctx)
	filteredTable, err := sortedTable.Where(ctx, "Vol >= 20000")
	if err != nil {
		fmt.Println("error when filtering:", err.Error())
		return
	}
	defer filteredTable.Release(ctx)

	// And if we want to see the data we can snapshot it to get a Record back.
	filteredRecord, err := filteredTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when filtering:", err.Error())
		return
	}
	defer filteredRecord.Release()

	fmt.Println("Data After:")
	fmt.Println(filteredRecord)
}

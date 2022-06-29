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

	// First, we need some Arrow record we want to upload.
	sampleRecord := common.GetExampleRecord()
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

	// Ff we want to see the data we sorted and filtered, we can snapshot the table to get a Record back.
	filteredRecord, err := filteredTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when filtering:", err.Error())
		return
	}
	defer filteredRecord.Release()

	fmt.Println("Data After:")
	fmt.Println(filteredRecord)
}

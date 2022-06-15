package examples

import (
	"context"
	"fmt"
	"time"

	"github.com/deephaven/deephaven-core/go-client/client"
)

// This example shows off how you can run a server-side script directly via the client,
// which can manipulate existing tables or create new ones.
func ExampleRunScript() {
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	// Note the choice of python as the script language here ("groovy" is the other option).
	cl, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}
	defer cl.Close()

	// First, let's create a new TimeTable with a 100ms period, starting a second ago
	startTime := time.Now().UnixNano() - int64(time.Second)
	timeTable, err := cl.TimeTable(ctx, 100_000_000, &startTime)
	if err != nil {
		fmt.Println("error when creating new time table:", err.Error())
		return
	}
	// Note that any tables you get a reference to must be released!
	defer timeTable.Release(ctx)

	// Next, let's bind it to a variable so we can use it in the script.
	// This also makes it visible to other clients or to the web UI.
	err = cl.BindToVariable(ctx, "my_example_table", timeTable)
	if err != nil {
		fmt.Println("error when binding table to variable:", err.Error())
		return
	}

	// Now, let's do some arbitrary operations on it...
	err = cl.RunScript(ctx,
		`
from deephaven.time import upper_bin
example_table_2 = my_example_table.update(["UpperBinned = upperBin(Timestamp, SECOND)"]).headBy(5, "UpperBinned")
`)
	if err != nil {
		fmt.Println("error when running script:", err.Error())
		return
	}

	// Now, we can open it to use locally.
	exampleTable, err := cl.OpenTable(ctx, "example_table_2")
	if err != nil {
		fmt.Println("error when opening table:", err.Error())
		return
	}
	// Don't forget to release it!
	defer exampleTable.Release(ctx)

	// And if we want to see what data is currently in it, we can take a snapshot.
	exampleSnapshot, err := exampleTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting table:", err.Error())
	}
	defer exampleSnapshot.Release()
	fmt.Println("Got table!")
	fmt.Println(exampleSnapshot)
}

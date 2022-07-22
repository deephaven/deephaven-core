package client_test

import (
	"context"
	"fmt"
	"time"

	"github.com/deephaven/deephaven-core/go/client/client"
	"github.com/deephaven/deephaven-core/go/client/internal/test_tools"
)

// This example shows how you can run a server-side script directly via the client
// and how you can use the script results in the client.
//
// This example requires a Deephaven server to connect to, so it will not work on pkg.go.dev.
func Example_runScript() {
	// A context is used to set timeouts and deadlines for requests or cancel requests.
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	// Let's start a client connection using python as the script language ("groovy" is the other option).
	// Note that the client language must match the language the server was started with.
	cl, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), client.WithConsole("python"))
	if err != nil {
		fmt.Println("error when connecting to server:", err.Error())
		return
	}
	defer cl.Close()

	// First, let's create a new TimeTable, starting one second ago, that gets a new row every 100 ms.
	startTime := time.Now().Add(time.Duration(-1) * time.Second)
	timeTable, err := cl.TimeTable(ctx, time.Duration(100)*time.Millisecond, startTime)
	if err != nil {
		fmt.Println("error when creating new time table:", err.Error())
		return
	}
	// Any tables you create should be eventually released.
	defer timeTable.Release(ctx)

	// Next, let's bind the table to a variable so we can use it in the script.
	// This also makes the table visible to other clients or to the web UI.
	err = cl.BindToVariable(ctx, "my_example_table", timeTable)
	if err != nil {
		fmt.Println("error when binding table to variable:", err.Error())
		return
	}

	// Now, let's run a script to do some arbitrary operations on my_example_table...
	err = cl.RunScript(ctx,
		`
from deephaven.time import upper_bin
example_table_2 = my_example_table.update(["UpperBinned = upperBin(Timestamp, SECOND)"]).head(5)
`)
	if err != nil {
		fmt.Println("error when running script:", err.Error())
		return
	}

	// Now, we can open example_table_2 to use locally.
	exampleTable2, err := cl.OpenTable(ctx, "example_table_2")
	if err != nil {
		fmt.Println("error when opening table:", err.Error())
		return
	}
	// Don't forget to release it!
	defer exampleTable2.Release(ctx)

	// And if we want to see what data is currently in example_table_2, we can take a snapshot.
	exampleSnapshot, err := exampleTable2.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting table:", err.Error())
		return
	}
	// Arrow records must also be released when not used anymore.
	defer exampleSnapshot.Release()

	fmt.Println("Got table snapshot!")
	fmt.Printf("It has %d rows and %d columns", exampleSnapshot.NumRows(), exampleSnapshot.NumCols())

	// Output:
	// Got table snapshot!
	// It has 5 rows and 2 columns
}

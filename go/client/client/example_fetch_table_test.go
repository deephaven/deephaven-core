package client_test

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go/client/client"
	"github.com/deephaven/deephaven-core/go/client/internal/test_tools"
)

// If you want to access tables from previous sessions or from the web UI,
// you will need to use OpenTable.
//
// This example requires a Deephaven server to connect to, so it will not work on pkg.go.dev.
func Example_fetchTable() {
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

	// First, let's make an empty table with ten rows.
	tbl, err := cl.EmptyTable(ctx, 10)
	if err != nil {
		fmt.Println("error when making table:", err.Error())
		return
	}

	// We can bind the table to a variable, so that it doesn't disappear when the client closes.
	err = cl.BindToVariable(ctx, "my_table", tbl)
	if err != nil {
		fmt.Println("error when binding table:", err.Error())
		return
	}

	// Now we can close the table and client locally, but the table will stick around on the server.
	tbl.Release(ctx)
	cl.Close()

	// Now let's make a new connection, completely unrelated to the old one.
	cl, err = client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort())
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}

	// Now, we can open the table from the previous session, and it will work fine.
	tbl, err = cl.OpenTable(ctx, "my_table")
	if err != nil {
		fmt.Println("error when opening table:", err.Error())
	}

	fmt.Println("Successfully opened the old table!")

	tbl.Release(ctx)
	cl.Close()

	// Output:
	// Successfully opened the old table!
}

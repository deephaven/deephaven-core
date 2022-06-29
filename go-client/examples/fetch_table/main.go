package main

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go-client/client"
)

// Typically, you don't have to worry about this,
// but if you want to access tables from previous sessions or from the web UI,
// you will need to do a table fetch.
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

	// First, let's make a table.
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
	cl, err = client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}

	// Now we have to fetch the list of tables. We use FetchOnce here because we
	// are going to immediately open the table afterwards and we don't care about
	// what other changes may happen in the future.
	err = cl.FetchTables(ctx, client.FetchOnce)
	if err != nil {
		fmt.Println("error when fetching tables:", err.Error())
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
}

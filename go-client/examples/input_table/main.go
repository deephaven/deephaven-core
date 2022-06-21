package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/examples/common"
)

// This example shows how to use Input Tables.
// Though Deephaven has direct support for streaming data through Apache Kafka,
// Input Tables make are a generic interface for streaming data from any source.
func main() {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}
	defer cl.Close()

	// First, let's make a schema our input table is going to use.
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Vol", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	// Then we can actually make the input table.
	// It will start empty, but we're going to add more data to it.
	inputTable, err := cl.NewKeyBackedInputTableFromSchema(ctx, schema, "Ticker")
	if err != nil {
		fmt.Println("error when creating InputTable", err.Error())
		return
	}
	defer inputTable.Release(ctx)

	// Now let's create a table derived from it.
	// When we update the input table, this table will update too.
	outputTable, err := inputTable.Where(ctx, "Close > 50.0")
	if err != nil {
		fmt.Println("error when filtering input table", err.Error())
		return
	}
	defer outputTable.Release(ctx)

	// Now, let's get some new data to add to the table.
	newDataRec := common.GetExampleRecord()
	defer newDataRec.Release()
	newDataTable, err := cl.ImportTable(ctx, newDataRec)
	if err != nil {
		fmt.Println("error when importing new data", err.Error())
		return
	}
	defer newDataTable.Release(ctx)

	// Now we can add the new data to our input table.
	err = inputTable.AddTable(ctx, newDataTable)
	if err != nil {
		fmt.Println("error when adding new data to table", err.Error())
		return
	}

	// Now we should be able to see the result of filtering the data we just added to the input table.
	outputRec, err := outputTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when snapshotting table", err.Error())
		return
	}
	defer outputRec.Release()

	fmt.Println("Got the output table!")
	fmt.Println(outputRec)
}

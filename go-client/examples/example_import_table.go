package examples

import (
	"context"
	"fmt"

	"github.com/deephaven/deephaven-core/go-client/client"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

func GetExampleRecord() arrow.Record {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Vol", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"}, nil)
	b.Field(1).(*array.Float32Builder).AppendValues([]float32{53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4}, nil)
	b.Field(2).(*array.Int32Builder).AppendValues([]int32{87000, 6060842, 138000, 138000000, 19000, 48300, 1500}, nil)

	return b.NewRecord()
}

// This example shows off the ability to upload tables to the Deephaven server,
// perform some operations on them,
// and then download them to access the modified data.
func ExampleImportTable() {
	// If you don't have any specific requirements, context.Background() is a good default.
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		fmt.Println("error when connecting to localhost port 10000:", err.Error())
		return
	}
	defer cl.Close()

	// First, we need some Record we want to upload.
	sampleRecord := GetExampleRecord()
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
	}
	defer sortedTable.Release(ctx)
	filteredTable, err := sortedTable.Where(ctx, "Vol >= 20000")
	if err != nil {
		fmt.Println("error when filtering:", err.Error())
	}
	defer filteredTable.Release(ctx)

	// And if we want to see the data we can snapshot it to get a Record back.
	filteredRecord, err := filteredTable.Snapshot(ctx)
	if err != nil {
		fmt.Println("error when filtering:", err.Error())
	}
	defer filteredRecord.Release()

	fmt.Println("Data After:")
	fmt.Println(filteredRecord)
}

// Miscellaneous helpful functions to make testing easier with less boilerplate.
package test_tools

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

// ExampleRecord creates an arrow Record with some arbitrary data, used for testing.
// The returned Record should be released when it is not needed anymore.
func ExampleRecord() arrow.Record {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Volume", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"}, nil)
	b.Field(1).(*array.Float32Builder).AppendValues([]float32{53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4}, nil)
	b.Field(2).(*array.Int32Builder).AppendValues([]int32{87000, 6060842, 138000, 138000000, 19000, 48300, 1500}, nil)

	return b.NewRecord()
}

// RandomRecord creates an arrow record with random int32 values.
// It will have the specified number of rows and columns, each entry will be in the range [0, maxNum).
func RandomRecord(numCols int, numRows int, maxNum int) arrow.Record {
	var fields []arrow.Field
	for col := 0; col < numCols; col += 1 {
		name := fmt.Sprintf("%c", 'a'+col)
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32})
	}

	schema := arrow.NewSchema(fields, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for col := 0; col < numCols; col += 1 {
		var arr []int32
		for row := 0; row < numRows; row += 1 {
			arr = append(arr, int32(rand.Intn(maxNum)))
		}

		b.Field(col).(*array.Int32Builder).AppendValues(arr, nil)
	}

	return b.NewRecord()
}

// CheckError prints the passed error and message and fails the test if the given error is not nil.
func CheckError(t *testing.T, msg string, err error) {
	if err != nil {
		t.Fatalf("%s error %s", msg, err.Error())
	}
}

// GetHost returns the host to connect to for the tests.
// By default it is localhost, but can be overriden by setting the DH_HOST environment variable.
func GetHost() string {
	host := os.Getenv("DH_HOST")
	if host == "" {
		return "localhost"
	} else {
		return host
	}
}

// GetPort returns the port to connect to for the tests.
// By default it is 10000, but can be overriden by setting the DH_PORT environment variable.
func GetPort() string {
	port := os.Getenv("DH_PORT")
	if port == "" {
		return "10000"
	} else {
		return port
	}
}

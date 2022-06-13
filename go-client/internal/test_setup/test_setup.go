package test_setup

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

func ExampleRecord() arrow.Record {
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

func RandomRecord(numCols int, numRows int, maxNum int) arrow.Record {
	pool := memory.NewGoAllocator()

	var fields []arrow.Field
	for col := 0; col < numCols; col += 1 {
		name := fmt.Sprintf("%c", 'a'+col)
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32})
	}

	schema := arrow.NewSchema(fields, nil)

	b := array.NewRecordBuilder(pool, schema)
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

func CheckError(t *testing.T, msg string, err error) {
	if err != nil {
		t.Fatalf("%s error %s", msg, err.Error())
	}
}

package common

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

// GetExampleRecord returns an Arrow record that contains some arbitrary data for use as an example.
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

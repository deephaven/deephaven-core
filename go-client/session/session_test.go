package session_test

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func exampleRecord() array.Record {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			//{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Vol", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	//b.Field(0).(*array.StringBuilder).AppendValues([]string{"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA", "T"}, nil)
	b.Field(0).(*array.Float32Builder).AppendValues([]float32{53.8, 88.5, 38.7, 453, 26.7, 544.9, 13.4}, nil)
	b.Field(1).(*array.Int32Builder).AppendValues([]int32{87000, 6060842, 138000, 138000000, 19000, 48300, 1500}, nil)

	return b.NewRecord()
}

/*
func TestTableUpload(t *testing.T) {
	r := exampleRecord()
	defer r.Release()

	fmt.Println("Record: ", r)

	schema := arrow.NewSchema(
		[]arrow.Field{
			//{Name: "Ticker", Type: arrow.BinaryTypes.String},
			{Name: "Close", Type: arrow.PrimitiveTypes.Float32},
			{Name: "Vol", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	tbl := array.NewTableFromRecords(schema, []array.Record{r})
	defer tbl.Release()

	ctx := context.Background()
	s, err := session.NewSession(ctx, "localhost", "10000")
	if err != nil {
		t.Errorf("Error %e", err)
	}
	defer s.Close(ctx)

	s.ImportTable(ctx, tbl)
}
*/

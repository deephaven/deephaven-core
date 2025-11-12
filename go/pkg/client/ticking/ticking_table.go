package ticking

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

type TickingStatus struct {
	Update TickingUpdate // Not present if err is non-nil
	Err    error
}

type tickingColumn interface {
	Get(row int) interface{}
}

type TickingTable struct {
	// FIXME: this doesn't handle nulls!

	schema  *arrow.Schema
	columns []tickingColumn

	keys *roaring64.Bitmap

	redirectIndex map[uint64]int // Maps from key-space IDs to an index in the column data.

	freeRows *roaring64.Bitmap // A list of column data indices that are no longer needed.
}

var ErrUnsupportedType = errors.New("unsupported data type for ticking table")

func NewTickingTable(schema *arrow.Schema) (*TickingTable, error) {
	var columns []tickingColumn
	for _, field := range schema.Fields() {
		var newColumn tickingColumn

		switch field.Type.ID() {
		case arrow.INT32:
			newColumn = &Int32Column{}
		case arrow.TIMESTAMP:
			newColumn = &TimestampColumn{}
		default:
			return nil, fmt.Errorf("unsupported data type for ticking table %v %T", field.Type, field.Type)
		}

		columns = append(columns, newColumn)
	}

	return &TickingTable{schema: schema, columns: columns, keys: roaring64.New(), freeRows: roaring64.New(), redirectIndex: make(map[uint64]int)}, nil
}

func inclusiveRange(start uint64, end uint64) *roaring64.Bitmap {
	keyRange := roaring64.New()
	if end == math.MaxUint64 {
		keyRange.AddRange(start, end)
		keyRange.Add(math.MaxUint64)
	} else {
		keyRange.AddRange(start, end+1)
	}
	return keyRange
}

func (tt *TickingTable) DeleteKeyRange(start uint64, end uint64) {
	keyRange := inclusiveRange(start, end)
	keyRange.And(tt.keys)
	iter := keyRange.Iterator()

	for iter.HasNext() {
		key := iter.Next()
		storageIndex := tt.redirectIndex[key]
		tt.freeRows.Add(uint64(storageIndex))
		delete(tt.redirectIndex, key)
	}

	tt.keys.RemoveRange(start, end+1)
}

func (tt *TickingTable) ShiftKeyRange(start uint64, end uint64, dest uint64) {
	keyRange := inclusiveRange(start, end)
	keyRange.And(tt.keys)

	var iter roaring64.IntIterable64
	if dest < start {
		// Negative deltas get applied in low-to-high keyspace order.
		iter = keyRange.Iterator()
	} else {
		// Positive deltas get applied in high-to-low keyspace order.
		iter = keyRange.ReverseIterator()
	}

	for iter.HasNext() {
		src := iter.Next()
		dst := dest + (src - start)

		storageIdx := tt.redirectIndex[src]
		delete(tt.redirectIndex, src)
		tt.redirectIndex[dst] = storageIdx

		tt.keys.Remove(src)
		tt.keys.Add(dst)
	}
}

func (tt *TickingTable) ApplyUpdate(update TickingUpdate) {
	var rowidx = 0

	for _, rowRange := range update.RemovedRows.Ranges {
		tt.DeleteKeyRange(rowRange.Begin, rowRange.End)
	}

	var startSet []uint64
	for r := range update.ShiftDataStarts.GetAllRows() {
		startSet = append(startSet, r)
	}

	var endSet []uint64
	for r := range update.ShiftDataEnds.GetAllRows() {
		endSet = append(endSet, r)
	}

	var destSet []uint64
	for r := range update.ShiftDataDests.GetAllRows() {
		destSet = append(destSet, r)
	}

	// Negative deltas get applied low-to-high keyspace order
	for i := 0; i < len(startSet); i++ {
		start := startSet[i]
		end := endSet[i]
		dest := destSet[i]

		if dest < start {
			tt.ShiftKeyRange(start, end, dest)
		}
	}

	// Positive deltas get applied high-to-low keyspace order
	for i := len(startSet) - 1; i >= 0; i-- {
		start := startSet[i]
		end := endSet[i]
		dest := destSet[i]

		if dest > start {
			tt.ShiftKeyRange(start, end, dest)
		}
	}

	if update.HasIncludedRows {
		for r := range update.AddedRowsIncluded.GetAllRows() {
			tt.AddRow(r, update.Record, rowidx)
			rowidx++
		}
	} else {
		for r := range update.AddedRows.GetAllRows() {
			tt.AddRow(r, update.Record, rowidx)
			rowidx++
		}
	}

	var modifiedRows [][]int64 = make([][]int64, len(tt.columns))
	if len(modifiedRows) != len(update.ModifiedRows) {
		panic("modified rows wrong length")
	}
	for i, m := range update.ModifiedRows {
		rowoffset := 0
		for r := range m.GetAllRows() { // be wary of goroutine leaks
			tt.ModifyEntry(i, r, update.Record, rowidx+rowoffset)
			rowoffset += 1
		}
	}

	// Should this method release the Record automatically?
	// Users can call Retain if they want to keep it around afterwards.
	update.Record.Release()
}

func (tt *TickingTable) String() string {
	o := new(strings.Builder)
	fmt.Fprintf(o, "ticking table:\n")
	fmt.Fprintf(o, "  %v\n", tt.schema)
	for idx, column := range tt.columns {
		name := tt.schema.Field(idx).Name
		fmt.Fprintf(o, "col[%d][%s]: [", idx, name)

		iter := tt.keys.Iterator()
		for iter.HasNext() {
			key := iter.Next()
			storage := tt.redirectIndex[key]
			fmt.Fprintf(o, "%v", column.Get(storage))
			if iter.HasNext() {
				fmt.Fprintf(o, ", ")
			}
		}
		fmt.Fprintf(o, "]\n")
	}

	return o.String()
}

// The returned record must be eventually released
func (tt *TickingTable) ToRecord() arrow.Record {
	b := array.NewRecordBuilder(memory.DefaultAllocator, tt.schema)
	defer b.Release()

	for fieldIdx := 0; fieldIdx < len(tt.columns); fieldIdx++ {
		iter := tt.keys.Iterator()

		switch tt.schema.Field(fieldIdx).Type.ID() {
		case arrow.INT32:
			sourceData := tt.columns[fieldIdx].(*Int32Column)
			orderedData := make([]int32, tt.keys.GetCardinality())
			i := 0
			for iter.HasNext() {
				key := iter.Next()
				storage := tt.redirectIndex[key]
				orderedData[i] = sourceData.values[storage]
				i++
			}
			b.Field(fieldIdx).(*array.Int32Builder).AppendValues(orderedData, nil)
		case arrow.TIMESTAMP:
			sourceData := tt.columns[fieldIdx].(*TimestampColumn)
			orderedData := make([]arrow.Timestamp, tt.keys.GetCardinality())
			i := 0
			for iter.HasNext() {
				key := iter.Next()
				storage := tt.redirectIndex[key]
				orderedData[i] = sourceData.values[storage]
				i++
			}
			b.Field(fieldIdx).(*array.TimestampBuilder).AppendValues(orderedData, nil)
		}
	}

	return b.NewRecord()
}

func (tt *TickingTable) ModifyEntry(column int, key uint64, sourceRecord arrow.Table, sourceRow int) {
	if !tt.schema.Equal(sourceRecord.Schema()) {
		panic("mismatched schema")
	}

	if column >= len(tt.columns) {
		panic("attempt to modify a nonexistent column")
	}

	storagePos, ok := tt.redirectIndex[key]
	if !ok {
		panic("attempt to modify a nonexistent row")
	}

	sourceColumn := sourceRecord.Column(column)
	destColumn := tt.columns[column]

	switch tt.schema.Field(column).Type.ID() {
	case arrow.INT32:
		destData := destColumn.(*Int32Column)
		destData.values[storagePos] = getI32Value(sourceColumn, sourceRow)
	case arrow.TIMESTAMP:
		destData := destColumn.(*TimestampColumn)
		destData.values[storagePos] = getTimestampValue(sourceColumn, sourceRow)
	}
}

func getI32Value(column *arrow.Column, sourceRow int) int32 {
	for _, chunk := range column.Data().Chunks() {
		arr := chunk.(*array.Int32)
		values := arr.Int32Values()
		if sourceRow < len(values) {
			return values[sourceRow]
		} else {
			sourceRow -= len(values)
		}
	}

	panic("out of bounds index")
}

func getTimestampValue(column *arrow.Column, sourceRow int) arrow.Timestamp {
	for _, chunk := range column.Data().Chunks() {
		arr := chunk.(*array.Timestamp)
		values := arr.TimestampValues()
		if sourceRow < len(values) {
			return values[sourceRow]
		} else {
			sourceRow -= len(values)
		}
	}

	panic("out of bounds index")
}

// TODO: This could be optimized by having it add an entire range at a time.
func (tt *TickingTable) AddRow(key uint64, sourceRecord arrow.Table, sourceRow int) {
	if !tt.schema.Equal(sourceRecord.Schema()) {
		panic("mismatched schema")
	}
	if tt.keys.Contains(key) {
		panic("trying to add a row with a key that already exists")
	}

	freeIndex := -1
	if !tt.freeRows.IsEmpty() {
		freeIndex = int(tt.freeRows.Maximum())
		tt.freeRows.Remove(uint64(freeIndex))
	}

	for colIdx := 0; colIdx < len(tt.columns); colIdx++ {
		sourceColumn := sourceRecord.Column(colIdx)

		var destIdx int

		switch tt.schema.Field(colIdx).Type.ID() {
		case arrow.INT32:
			destData := tt.columns[colIdx].(*Int32Column)
			sourceValue := getI32Value(sourceColumn, sourceRow)
			if freeIndex == -1 {
				destIdx = len(destData.values)
				destData.values = append(destData.values, sourceValue)
			} else {
				destIdx = freeIndex
				destData.values[freeIndex] = sourceValue
			}
		case arrow.TIMESTAMP:
			destData := tt.columns[colIdx].(*TimestampColumn)
			sourceValue := getTimestampValue(sourceColumn, sourceRow)
			if freeIndex == -1 {
				destIdx = len(destData.values)
				destData.values = append(destData.values, sourceValue)
			} else {
				destIdx = freeIndex
				destData.values[freeIndex] = sourceValue
			}
		default:
			panic("unsupported type")
		}

		tt.keys.Add(key)
		tt.redirectIndex[key] = destIdx
	}
}

// TODO:
// This would be a more efficient way to consume the record instead of using rowidx in ApplyUpdate
// Is it worth the extra effort? Who knows!
type ChunkedIter struct {
	chunks   *arrow.Chunked
	chunkIdx int
	elemIdx  int
}

// you'd have one of these methods for each of the supported types, which seems... like a lot of work
func (iter *ChunkedIter) NextI32() (value int32, ok bool) {
	if iter.chunkIdx == len(iter.chunks.Chunks()) {
		return 0, false
	} else {
		chunk := iter.chunks.Chunk(iter.chunkIdx).(*array.Int32)
		value := chunk.Value(iter.elemIdx)
		iter.elemIdx++
		if iter.elemIdx == chunk.Len() {
			iter.elemIdx = 0
			iter.chunkIdx += 1
		}
		return value, true
	}
}

// ditto on the above comment
// also the length of the returned array might not necessarily be the same as the len argument
func (iter *ChunkedIter) NextI32Slice(length int) (values []int32) {
	if iter.chunkIdx == len(iter.chunks.Chunks()) {
		return nil
	} else {
		chunk := iter.chunks.Chunk(iter.chunkIdx).(*array.Int32)

		consumed := length
		if consumed > chunk.Len() {
			consumed = chunk.Len()
		}

		values = chunk.Int32Values()[iter.elemIdx : iter.elemIdx+consumed]

		iter.elemIdx += consumed
		if iter.elemIdx == chunk.Len() {
			iter.elemIdx = 0
			iter.chunkIdx += 1
		}

		return values
	}
}

type Int32Column struct {
	values []int32
}

func (col *Int32Column) Get(row int) interface{} {
	return col.values[row]
}

type TimestampColumn struct {
	// TODO: Mark whether or not this is a ns, ms, etc. column

	values []arrow.Timestamp
}

func (col *TimestampColumn) Get(row int) interface{} {
	return col.values[row]
}

type TickingUpdate struct {
	Record arrow.Table

	IsSnapshot bool

	AddedRows   RowSet
	RemovedRows RowSet

	AddedRowsIncluded RowSet
	HasIncludedRows   bool

	ShiftDataStarts RowSet
	ShiftDataEnds   RowSet
	ShiftDataDests  RowSet

	ModifiedRows []RowSet
}

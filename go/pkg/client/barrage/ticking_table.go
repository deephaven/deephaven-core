package barrage

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
)

type tickingColumn interface {
	Get(row int) interface{}
}

type TickingTable struct {
	schema  *arrow.Schema
	columns []tickingColumn

	redirectIndex map[int64]int // Maps from key-space IDs to an index in the column data.
	freeRows      []int         // A list of column data indices that are no longer needed.
}

var ErrUnsupportedType = errors.New("unsupported data type for ticking table")

func NewTickingTable(schema *arrow.Schema) (TickingTable, error) {
	var columns []tickingColumn
	for _, field := range schema.Fields() {
		var newColumn tickingColumn

		switch field.Type.ID() {
		case arrow.PrimitiveTypes.Int32.ID():
			newColumn = &Int32Column{}
		case arrow.FixedWidthTypes.Timestamp_ns.ID(): // TODO: There has to be a better way to compare these.
			newColumn = &TimestampColumn{}
		default:
			return TickingTable{}, fmt.Errorf("unsupported data type for ticking table %v %T", field.Type, field.Type)
		}

		columns = append(columns, newColumn)
	}

	return TickingTable{schema: schema, columns: columns, redirectIndex: make(map[int64]int)}, nil
}

func (tt *TickingTable) DeleteKeyRange(start int64, end int64) {
	for key := start; key <= end; key++ {
		if dataIndex, ok := tt.redirectIndex[key]; ok {
			tt.freeRows = append(tt.freeRows, dataIndex)
			delete(tt.redirectIndex, key)
		}
	}
}

func (tt *TickingTable) ShiftKeyRange(start int64, end int64, dest int64) {
	if dest < start {
		// Negative deltas get applied in low-to-high keyspace order.
		for off := int64(0); off <= end-start; off++ {
			src := start + off
			dst := dest + off

			if dataIndex, ok := tt.redirectIndex[src]; ok {
				delete(tt.redirectIndex, src)
				tt.redirectIndex[dst] = dataIndex
			}
		}
	} else if dest > start {
		// Positive deltas get applied in high-to-low keyspace order.
		for off := end - start; off >= 0; off-- {
			src := start + off
			dst := dest + off

			if dataIndex, ok := tt.redirectIndex[src]; ok {
				delete(tt.redirectIndex, src)
				tt.redirectIndex[dst] = dataIndex
			}
		}
	}
}

type idPair struct {
	key  int64
	data int
}

type idPairSorter struct {
	pairs []idPair
}

func (ips *idPairSorter) Len() int {
	return len(ips.pairs)
}

func (ips *idPairSorter) Swap(i, j int) {
	ips.pairs[i], ips.pairs[j] = ips.pairs[j], ips.pairs[i]
}

func (ips *idPairSorter) Less(i, j int) bool {
	return ips.pairs[i].key < ips.pairs[j].key
}

func (tt *TickingTable) getDataIndices() []int {
	var pairs []idPair
	for key, data := range tt.redirectIndex {
		pairs = append(pairs, idPair{key: key, data: data})
	}
	sorter := idPairSorter{pairs: pairs}
	sort.Sort(&sorter)

	var indices []int
	for _, pair := range sorter.pairs {
		indices = append(indices, pair.data)
	}
	return indices
}

func (tt *TickingTable) String() string {
	idxs := tt.getDataIndices()

	o := new(strings.Builder)
	fmt.Fprintf(o, "ticking table:\n")
	fmt.Fprintf(o, "  %v\n", tt.schema)
	for idx, column := range tt.columns {
		name := tt.schema.Field(idx).Name
		fmt.Fprintf(o, "col[%d][%s]: [", idx, name)
		for iIdx, dataIdx := range idxs {
			fmt.Fprintf(o, "%v", column.Get(dataIdx))
			if iIdx != len(idxs)-1 {
				fmt.Fprintf(o, ", ")
			}
		}
		fmt.Fprintf(o, "]\n")
	}

	return o.String()
}

// TODO: This could be optimized by having it add an entire range at a time.
func (tt *TickingTable) AddRow(key int64, sourceRecord arrow.Record, sourceRow int) {
	if !tt.schema.Equal(sourceRecord.Schema()) {
		panic("mismatched schema")
	}

	freeIndex := -1
	if len(tt.freeRows) > 0 {
		freeIndex = tt.freeRows[0]
		tt.freeRows = tt.freeRows[1:]
	}

	for colIdx := 0; colIdx < len(tt.columns); colIdx++ {
		sourceColumn := sourceRecord.Column(colIdx)

		var destIdx int

		switch tt.schema.Field(colIdx).Type.ID() {
		case arrow.PrimitiveTypes.Int32.ID():
			sourceData := sourceColumn.(*array.Int32)
			destData := tt.columns[colIdx].(*Int32Column)
			sourceValue := sourceData.Value(sourceRow)
			if freeIndex == -1 {
				destIdx = len(destData.values)
				destData.values = append(destData.values, sourceValue)
			} else {
				destIdx = freeIndex
				destData.values[freeIndex] = sourceValue
			}
		case arrow.FixedWidthTypes.Timestamp_ns.ID(): // TODO: There has to be a better way to compare these
			sourceData := sourceColumn.(*array.Timestamp)
			destData := tt.columns[colIdx].(*TimestampColumn)
			sourceValue := sourceData.Value(sourceRow)
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

		tt.redirectIndex[key] = destIdx
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

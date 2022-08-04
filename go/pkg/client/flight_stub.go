package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/ipc"
	flatbuffers "github.com/google/flatbuffers/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ticketpb2 "github.com/deephaven/deephaven-core/go/internal/proto/ticket"

	flatbuf_b "github.com/deephaven/deephaven-core/go/internal/barrage/flatbuf"
	flatbuf_a "github.com/deephaven/deephaven-core/go/org/apache/arrow/flatbuf"
)

// flightStub wraps Arrow Flight gRPC calls.
type flightStub struct {
	client *Client

	stub flight.Client // The stub for performing Arrow Flight gRPC requests.
}

func newFlightStub(client *Client, host string, port string) (flightStub, error) {
	stub, err := flight.NewClientWithMiddleware(
		net.JoinHostPort(host, port),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return flightStub{}, err
	}

	return flightStub{client: client, stub: stub}, nil
}

// snapshotRecord downloads the data currently in the provided table and returns it as an Arrow Record.
func (fs *flightStub) snapshotRecord(ctx context.Context, ticket *ticketpb2.Ticket) (arrow.Record, error) {
	ctx, err := fs.client.withToken(ctx)
	if err != nil {
		return nil, err
	}

	fticket := &flight.Ticket{Ticket: ticket.GetTicket()}

	req, err := fs.stub.DoGet(ctx, fticket)
	if err != nil {
		return nil, err
	}
	defer req.CloseSend()

	reader, err := flight.NewRecordReader(req)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	rec1, err := reader.Read()
	if err != nil {
		return nil, err
	}
	rec1.Retain()

	rec2, err := reader.Read()
	if err != io.EOF {
		rec1.Release()
		rec2.Release()
		return nil, errors.New("multiple records retrieved during snapshot")
	}

	return rec1, nil
}

// ImportTable uploads a table to the Deephaven server.
// The table can then be manipulated and referenced using the returned TableHandle.
func (fs *flightStub) ImportTable(ctx context.Context, rec arrow.Record) (*TableHandle, error) {
	ctx, err := fs.client.withToken(ctx)
	if err != nil {
		return nil, err
	}

	doPut, err := fs.stub.DoPut(ctx)
	if err != nil {
		return nil, err
	}
	defer doPut.CloseSend()

	ticketNum := fs.client.ticketFact.nextId()

	//todo Seems like this should be a fixed size int cast here and not a generic int
	descr := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"export", strconv.Itoa(int(ticketNum))}}

	writer := flight.NewRecordWriter(doPut, ipc.WithSchema(rec.Schema()))

	writer.SetFlightDescriptor(descr)
	err = writer.Write(rec)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	_, err = doPut.Recv()
	if err != nil {
		return nil, err
	}

	ticket := fs.client.ticketFact.makeTicket(ticketNum)

	schema := rec.Schema()

	return newTableHandle(fs.client, &ticket, schema, rec.NumRows(), true), nil
}

// Close closes the flight stub and frees any associated resources.
// The flight stub should not be used after calling this function.
// The client lock should be held when calling this function.
func (fs *flightStub) Close() error {
	if fs.stub != nil {
		err := fs.stub.Close()
		if err != nil {
			return err
		}
		fs.stub = nil
	}
	return nil
}

func consumeRowSet(offsets []int64, addRowsInRange func(start int64, end int64), addRowAt func(offset int64)) {
	pending := int64(-1)
	lastValue := int64(0)

	consume := func(nextOffset int64) {
		if nextOffset < 0 {
			if pending == -1 {
				panic("consecutive negative values")
			}
			lastValue = lastValue - nextOffset
			addRowsInRange(pending, lastValue)
			pending = -1
		} else {
			if pending != -1 {
				addRowAt(pending)
			}
			lastValue += nextOffset
			pending = lastValue
		}
	}

	for _, offset := range offsets {
		consume(offset)
	}

	if pending != -1 {
		addRowAt(pending)
	}
}

type RowSetDecoder struct {
	bytes []byte
	index int

	resultRowSet []int64
}

func (dec *RowSetDecoder) munchByte() int8 {
	value := int8(dec.bytes[dec.index])
	dec.index += 1
	return value
}

func (dec *RowSetDecoder) munchShort() int16 {
	value := int16(binary.LittleEndian.Uint16(dec.bytes[dec.index : dec.index+2]))
	dec.index += 2
	return value
}

func (dec *RowSetDecoder) munchInt() int32 {
	value := int32(binary.LittleEndian.Uint32(dec.bytes[dec.index : dec.index+4]))
	dec.index += 4
	return value
}

func (dec *RowSetDecoder) munchLong() int64 {
	value := int64(binary.LittleEndian.Uint64(dec.bytes[dec.index : dec.index+8]))
	dec.index += 8
	return value
}

func (dec *RowSetDecoder) munchValue(valueType byte) (int64, error) {
	switch valueType {
	case 1: // short
		value := dec.munchShort()
		fmt.Print("s ", value, " ")
		return int64(value), nil
	case 2: // int
		value := dec.munchInt()
		fmt.Print("i ", value, " ")
		return int64(value), nil
	case 3: // long
		value := dec.munchLong()
		fmt.Print("l ", value, " ")
		return int64(value), nil
	case 4: // byte
		value := dec.munchByte()
		fmt.Print("b ", value, " ")
		return int64(value), nil
	default:
		return 0, fmt.Errorf("unknown RowSet value type %d", valueType)
	}
}

func (dec *RowSetDecoder) munchCmd() (done bool, err error) {
	cmd := byte(dec.munchByte())
	cmdType := cmd >> 3
	valType := cmd & 0x7

	switch cmdType {
	case 1:
		fmt.Print("offset ")
		val, err := dec.munchValue(valType)
		if err != nil {
			return false, err
		}
		dec.resultRowSet = append(dec.resultRowSet, int64(val))
		return false, nil
	case 2:
		fmt.Print("shortarray ")
		length, err := dec.munchValue(valType)
		if err != nil {
			return false, err
		}
		var values []int64
		for j := int64(0); j < length; j++ {
			value := int64(dec.munchShort())
			values = append(values, value)
		}
		fmt.Printf("%v ", values)
		dec.resultRowSet = append(dec.resultRowSet, values...)
		return false, nil
	case 3:
		fmt.Print("bytearray ")
		length, err := dec.munchValue(valType)
		if err != nil {
			return false, err
		}
		var values []int64
		for j := int64(0); j < length; j++ {
			value := int64(dec.munchByte())
			values = append(values, value)
		}
		fmt.Printf("%v ", values)
		dec.resultRowSet = append(dec.resultRowSet, values...)
		return false, nil
	case 4:
		fmt.Print("end")
		if valType != 0 {
			fmt.Printf(" (%d)", valType)
		}
		return true, nil
	default:
		return false, fmt.Errorf("unknown command type %d", cmdType)
	}
}

func (dec *RowSetDecoder) munchRowSet() ([]int64, error) {
	dec.resultRowSet = nil

	for {
		done, err := dec.munchCmd()
		if err != nil {
			return nil, err
		}

		if done {
			break
		}
	}

	return dec.resultRowSet, nil
}

func decodeRowSet(bytes []byte) ([]int64, error) {
	dec := RowSetDecoder{bytes: bytes}

	rowSet, err := dec.munchRowSet()
	if err != nil {
		return nil, err
	}

	if dec.index != len(dec.bytes) {
		return nil, errors.New("trailing bytes in encoded RowSet")
	}

	return rowSet, nil
}

func decodeRowSetShiftData(bytes []byte) (starts []int64, ends []int64, dests []int64, err error) {
	dec := RowSetDecoder{bytes: bytes}

	starts, err = dec.munchRowSet()
	if err != nil {
		return nil, nil, nil, err
	}

	ends, err = dec.munchRowSet()
	if err != nil {
		return nil, nil, nil, err
	}

	dests, err = dec.munchRowSet()
	if err != nil {
		return nil, nil, nil, err
	}

	if dec.index != len(dec.bytes) {
		return nil, nil, nil, errors.New("trailing bytes in encoded RowSetShiftData")
	}

	return starts, ends, dests, nil
}

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

func newTickingTable(schema *arrow.Schema) (TickingTable, error) {
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

	fmt.Println("idxs: ", idxs)

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

func (fs *flightStub) Subscribe(ctx context.Context, handle *TableHandle) (*int, error) {
	ctx, err := fs.client.withToken(ctx)
	if err != nil {
		return nil, err
	}

	doExchg, err := fs.stub.DoExchange(ctx)
	if err != nil {
		return nil, err
	}
	defer doExchg.CloseSend()

	builder_b := flatbuffers.NewBuilder(0)
	/*flatbuf_b.BarrageSubscriptionOptionsStart(builder_b)
	flatbuf_b.BarrageSubscriptionOptionsAddUseDeephavenNulls(builder_b, false)
	flatbuf_b.BarrageSubscriptionOptionsAddBatchSize(builder_b, 4096)
	flatbuf_b.BarrageSubscriptionOptionsAddMaxMessageSize(builder_b, 2000000000)
	opts := flatbuf_b.BarrageSubscriptionOptionsEnd(builder_b)*/

	flatbuf_b.BarrageSubscriptionRequestStartTicketVector(builder_b, len(handle.ticket.Ticket))
	for i := len(handle.ticket.Ticket) - 1; i >= 0; i-- {
		builder_b.PrependByte(handle.ticket.Ticket[i])
	}
	ticketVec := builder_b.EndVector(len(handle.ticket.Ticket))
	/*flatbuf_b.BarrageSubscriptionRequestStartColumnsVector(builder_b, 0)
	columnVec := builder_b.EndVector(0)
	flatbuf_b.BarrageSubscriptionRequestStartViewportVector(builder_b, 0)
	viewportVec := builder_b.EndVector(0)*/
	flatbuf_b.BarrageSubscriptionRequestStart(builder_b)
	flatbuf_b.BarrageSubscriptionRequestAddTicket(builder_b, ticketVec)
	//flatbuf_b.BarrageSubscriptionRequestAddColumns(builder_b, columnVec)
	//flatbuf_b.BarrageSubscriptionRequestAddViewport(builder_b, viewportVec)
	flatbuf_b.BarrageSubscriptionRequestAddReverseViewport(builder_b, false)
	//flatbuf_b.BarrageSubscriptionRequestAddSubscriptionOptions(builder_b, opts)
	bsrOff := flatbuf_b.BarrageSubscriptionRequestEnd(builder_b)
	builder_b.Finish(bsrOff)
	bsrPayload := builder_b.FinishedBytes()

	builder_c := flatbuffers.NewBuilder(0)
	flatbuf_b.BarrageMessageWrapperStartMsgPayloadVector(builder_c, len(bsrPayload))
	for i := len(bsrPayload) - 1; i >= 0; i-- {
		builder_c.PrependByte(bsrPayload[i])
	}
	bsrPayloadVec := builder_c.EndVector(len(bsrPayload))
	flatbuf_b.BarrageMessageWrapperStart(builder_c)
	flatbuf_b.BarrageMessageWrapperAddMagic(builder_c, 0x6E687064)
	flatbuf_b.BarrageMessageWrapperAddMsgType(builder_c, flatbuf_b.BarrageMessageTypeBarrageSubscriptionRequest)
	flatbuf_b.BarrageMessageWrapperAddMsgPayload(builder_c, bsrPayloadVec)
	bmwOff := flatbuf_b.BarrageMessageWrapperEnd(builder_c)
	builder_c.Finish(bmwOff)
	customMeta := builder_c.FinishedBytes()

	builder_a := flatbuffers.NewBuilder(0)
	flatbuf_a.MessageStart(builder_a)
	flatbuf_a.MessageAddHeaderType(builder_a, flatbuf_a.MessageHeaderNONE)
	msgOff := flatbuf_a.MessageEnd(builder_a)
	builder_a.Finish(msgOff)
	msgBuf := builder_a.FinishedBytes()

	desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte{0x64, 0x70, 0x68, 0x6E}}
	data := &flight.FlightData{DataHeader: msgBuf, AppMetadata: customMeta, FlightDescriptor: desc}

	msg := flatbuf_b.GetRootAsBarrageMessageWrapper(customMeta, 0)
	fmt.Println(msg)

	err = doExchg.Send(data)
	if err != nil {
		return nil, err
	}

	reader, err := flight.NewRecordReader(doExchg)
	if err != nil {
		return nil, err
	}

	tbl, err := newTickingTable(handle.schema)
	if err != nil {
		return nil, err
	}

	for reader.Next() {
		if reader.Err() != nil {
			return nil, err
		}

		record := reader.Record()

		fmt.Println(record)

		resp := reader.LatestAppMetadata()

		if len(resp) > 0 {
			meta := resp
			msgWrapper := flatbuf_b.GetRootAsBarrageMessageWrapper(meta, 0)

			payload := make([]byte, msgWrapper.MsgPayloadLength())
			for i := 0; i < msgWrapper.MsgPayloadLength(); i++ {
				payload[i] = byte(msgWrapper.MsgPayload(i))
			}

			fmt.Println(msgWrapper.MsgType())
			if msgWrapper.MsgType() == flatbuf_b.BarrageMessageTypeBarrageUpdateMetadata {
				updateMeta := flatbuf_b.GetRootAsBarrageUpdateMetadata(payload, 0)
				fmt.Println()
				fmt.Println("first_seq:", updateMeta.FirstSeq())
				fmt.Println("last_seq:", updateMeta.LastSeq())
				fmt.Println("is_snapshot", updateMeta.IsSnapshot())

				addedRows := make([]byte, updateMeta.AddedRowsLength())
				for i := 0; i < updateMeta.AddedRowsLength(); i++ {
					addedRows[i] = byte(updateMeta.AddedRows(i))
				}

				removedRows := make([]byte, updateMeta.RemovedRowsLength())
				for i := 0; i < updateMeta.RemovedRowsLength(); i++ {
					removedRows[i] = byte(updateMeta.RemovedRows(i))
				}

				addedRowsIncluded := make([]byte, updateMeta.AddedRowsIncludedLength())
				for i := 0; i < updateMeta.AddedRowsIncludedLength(); i++ {
					addedRowsIncluded[i] = byte(updateMeta.AddedRowsIncluded(i))
				}

				shiftData := make([]byte, updateMeta.ShiftDataLength())
				for i := 0; i < updateMeta.ShiftDataLength(); i++ {
					shiftData[i] = byte(updateMeta.ShiftData(i))
				}

				rowidx := 0

				fmt.Print("removed rows: ")
				removedRowsDec, err := decodeRowSet(removedRows)
				fmt.Println()
				if err != nil {
					fmt.Println(err)
					continue
				}
				consumeRowSet(removedRowsDec,
					func(start int64, end int64) {
						tbl.DeleteKeyRange(start, end)
					},
					func(offset int64) {
						tbl.DeleteKeyRange(offset, offset)
					})

				makeAppender := func(arr *[]int64) (func(start int64, end int64), func(offset int64)) {
					rangeApp := func(start int64, end int64) {
						for i := start; i <= end; i++ {
							*arr = append(*arr, i)
						}
					}
					offsetApp := func(offset int64) {
						*arr = append(*arr, offset)
					}
					return rangeApp, offsetApp
				}

				fmt.Print("shift data: ")
				starts, ends, dests, err := decodeRowSetShiftData(shiftData)
				fmt.Println()
				if err != nil {
					fmt.Println(err)
					continue
				}

				var startSet []int64
				ssA, ssB := makeAppender(&startSet)
				consumeRowSet(starts, ssA, ssB)

				var endSet []int64
				esA, esB := makeAppender(&endSet)
				consumeRowSet(ends, esA, esB)

				var destSet []int64
				dsA, dsB := makeAppender(&destSet)
				consumeRowSet(dests, dsA, dsB)

				fmt.Println("starts: ", startSet)
				fmt.Println("ends: ", endSet)
				fmt.Println("dests: ", destSet)

				if len(startSet) != len(endSet) || len(endSet) != len(destSet) {
					panic("mismatched sets")
				}

				// Negative deltas get applied low-to-high keyspace order
				for i := 0; i < len(startSet); i++ {
					start := startSet[i]
					end := endSet[i]
					dest := destSet[i]

					if dest < start {
						tbl.ShiftKeyRange(start, end, dest)
					}
				}

				// Positive deltas get applied high-to-low keyspace order
				for i := len(startSet) - 1; i >= 0; i-- {
					start := startSet[i]
					end := endSet[i]
					dest := destSet[i]

					if dest > start {
						tbl.ShiftKeyRange(start, end, dest)
					}
				}

				fmt.Print("added rows: ")
				addedRowsDec, err := decodeRowSet(addedRows)
				fmt.Println()
				if err != nil {
					fmt.Println(err)
				}
				consumeRowSet(addedRowsDec,
					func(start int64, end int64) {
						fmt.Printf("start: %d end: %d\n", start, end)
						for i := start; i <= end; i++ {
							tbl.AddRow(i, record, rowidx)
							rowidx++
						}
					},
					func(offset int64) {
						tbl.AddRow(offset, record, rowidx)
						rowidx++
					})

				// TODO: What do these even mean?
				//fmt.Printf("added rows inc (%d): ", len(addedRowsIncluded))
				//decodeRowSet(addedRowsIncluded)

				fmt.Println(&tbl)
			}
		}
	}

	return nil, err
}

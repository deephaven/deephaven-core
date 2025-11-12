package ticking

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type RowRange struct {
	Begin uint64
	End   uint64
}

func (rr RowRange) Size() uint64 {
	return rr.End - rr.Begin + 1
}

type RowSet struct {
	Ranges []RowRange
}

func (rs RowSet) Size() uint64 {
	var total uint64
	for _, r := range rs.Ranges {
		total += r.Size()
	}
	return total
}

func (rs *RowSet) GetAllRows() <-chan uint64 {
	ch := make(chan uint64)

	go func() {
		for _, r := range rs.Ranges {
			for i := r.Begin; i <= r.End; i++ {
				ch <- i
			}
		}
		close(ch)
	}()

	return ch
}

func ConsumeRowSet(offsets []int64, addRowsInRange func(start uint64, end uint64), addRowAt func(offset uint64)) {
	pending := int64(-1)
	lastValue := int64(0)

	consume := func(nextOffset int64) {
		if nextOffset < 0 {
			if pending == -1 {
				panic("consecutive negative values")
			}
			lastValue = lastValue - nextOffset
			addRowsInRange(uint64(pending), uint64(lastValue))
			pending = -1
		} else {
			if pending != -1 {
				addRowAt(uint64(pending))
			}
			lastValue += nextOffset
			pending = lastValue
		}
	}

	for _, offset := range offsets {
		consume(offset)
	}

	if pending != -1 {
		addRowAt(uint64(pending))
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

type rowSetCmdType byte

const (
	cmdTypeOffset     rowSetCmdType = 1
	cmdTypeShortArray rowSetCmdType = 2
	cmdTypeByteArray  rowSetCmdType = 3
	cmdTypeEnd        rowSetCmdType = 4
)

type rowSetValueType byte

const (
	valueTypeShort rowSetValueType = 1
	valueTypeInt   rowSetValueType = 2
	valueTypeLong  rowSetValueType = 3
	valueTypeByte  rowSetValueType = 4
)

func splitCommand(cmd byte) (rowSetCmdType, rowSetValueType) {
	cmdType := rowSetCmdType(cmd >> 3)
	valType := rowSetValueType(cmd & 0x7)
	return cmdType, valType
}

func makeCommand(cmdType rowSetCmdType, valType rowSetValueType) byte {
	return (byte(cmdType) << 3) | byte(valType)
}

func (dec *RowSetDecoder) munchValue(valueType rowSetValueType) (int64, error) {
	switch valueType {
	case valueTypeShort:
		value := dec.munchShort()
		return int64(value), nil
	case valueTypeInt:
		value := dec.munchInt()
		return int64(value), nil
	case valueTypeLong:
		value := dec.munchLong()
		return int64(value), nil
	case valueTypeByte:
		value := dec.munchByte()
		return int64(value), nil
	default:
		return 0, fmt.Errorf("unknown RowSet value type %d", valueType)
	}
}

func (dec *RowSetDecoder) munchCmd() (done bool, err error) {
	cmd := byte(dec.munchByte())
	cmdType, valType := splitCommand(cmd)

	switch cmdType {
	case cmdTypeOffset:
		val, err := dec.munchValue(valType)
		if err != nil {
			return false, err
		}
		dec.resultRowSet = append(dec.resultRowSet, int64(val))
		return false, nil
	case cmdTypeShortArray:
		length, err := dec.munchValue(valType)
		if err != nil {
			return false, err
		}
		var values []int64
		for j := int64(0); j < length; j++ {
			value := int64(dec.munchShort())
			values = append(values, value)
		}
		dec.resultRowSet = append(dec.resultRowSet, values...)
		return false, nil
	case cmdTypeByteArray:
		length, err := dec.munchValue(valType)
		if err != nil {
			return false, err
		}
		var values []int64
		for j := int64(0); j < length; j++ {
			value := int64(dec.munchByte())
			values = append(values, value)
		}
		dec.resultRowSet = append(dec.resultRowSet, values...)
		return false, nil
	case cmdTypeEnd:
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

func DecodeRowSet(bytes []byte) ([]int64, error) {
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

func DecodeRowSetShiftData(bytes []byte) (starts []int64, ends []int64, dests []int64, err error) {
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

// Converts a serialized RowSet into an array of row keys.
// Note that this is not as efficient as using ConsumeRowSet
// since it has to represent the entire set of keys in memory.
func DeserializeRowSet(bytes []byte) (RowSet, error) {
	decoded, err := DecodeRowSet(bytes)
	if err != nil {
		return RowSet{}, err
	}

	var result []RowRange
	ConsumeRowSet(decoded, func(start uint64, end uint64) {
		result = append(result, RowRange{Begin: start, End: end})
	}, func(index uint64) {
		result = append(result, RowRange{Begin: index, End: index})
	})

	return RowSet{Ranges: result}, nil
}

// Converts a serialized RowSetShiftData into 3 arrays of row keys.
// Note that this is not as efficient as using ConsumeRowSet
// since it has to represent the entire set of keys in memory.
func DeserializeRowSetShiftData(bytes []byte) (starts RowSet, ends RowSet, dests RowSet, err error) {
	startsDec, endsDec, destsDec, err := DecodeRowSetShiftData(bytes)
	if err != nil {
		return RowSet{}, RowSet{}, RowSet{}, err
	}

	makeAppender := func(arr *RowSet) (func(start uint64, end uint64), func(offset uint64)) {
		rangeApp := func(start uint64, end uint64) {
			arr.Ranges = append(arr.Ranges, RowRange{Begin: start, End: end})
		}
		offsetApp := func(offset uint64) {
			arr.Ranges = append(arr.Ranges, RowRange{Begin: offset, End: offset})
		}
		return rangeApp, offsetApp
	}

	startsA, startsB := makeAppender(&starts)
	ConsumeRowSet(startsDec, startsA, startsB)

	endsA, endsB := makeAppender(&ends)
	ConsumeRowSet(endsDec, endsA, endsB)

	destsA, destsB := makeAppender(&dests)
	ConsumeRowSet(destsDec, destsA, destsB)

	return starts, ends, dests, nil
}

type RowSetCompressor struct {
	offsets    []int64
	lastOffset int64
}

func NewRowSetCompressor() RowSetCompressor {
	return RowSetCompressor{}
}

func (rsc *RowSetCompressor) AddRow(key int64) {
	if key < rsc.lastOffset {
		panic("keys must be increasing")
	}

	keyOffset := key - rsc.lastOffset
	rsc.offsets = append(rsc.offsets, keyOffset)
	rsc.lastOffset = key
}

func (rsc *RowSetCompressor) AddRowRange(start int64, end int64) {
	if start < rsc.lastOffset {
		panic("keys must be increasing")
	}
	if start > end {
		panic("range must be increasing")
	}

	if start == end {
		rsc.AddRow(start)
	} else {
		startOffset := start - rsc.lastOffset
		endOffset := -(end - start)
		rsc.offsets = append(rsc.offsets, startOffset, endOffset)
		rsc.lastOffset = end
	}
}

type RowSetEncoder struct {
	bytes []byte
}

func NewRowSetEncoder() RowSetEncoder {
	return RowSetEncoder{}
}

func (rse *RowSetEncoder) AddOffset(offset int64) {
	if rse.IsFinished() {
		panic("tried to add an offset to a finished row set")
	}

	if math.MinInt8 <= offset && offset <= math.MaxInt8 {
		cmd := makeCommand(cmdTypeOffset, valueTypeByte)
		rse.bytes = append(rse.bytes, cmd, byte(offset))
	} else if math.MinInt16 <= offset && offset <= math.MaxInt16 {
		cmd := makeCommand(cmdTypeOffset, valueTypeShort)
		rse.bytes = append(rse.bytes, cmd, 0, 0)
		binary.LittleEndian.PutUint16(rse.bytes[len(rse.bytes)-2:], uint16(offset))
	} else if math.MinInt32 <= offset && offset <= math.MaxInt32 {
		cmd := makeCommand(cmdTypeOffset, valueTypeInt)
		rse.bytes = append(rse.bytes, cmd, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(rse.bytes[len(rse.bytes)-4:], uint32(offset))
	} else {
		cmd := makeCommand(cmdTypeOffset, valueTypeLong)
		rse.bytes = append(rse.bytes, cmd, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(rse.bytes[len(rse.bytes)-8:], uint64(offset))
	}
}

func (rse *RowSetEncoder) Finish() []byte {
	if rse.IsFinished() {
		panic("can't finish a row set twice")
	}

	rse.bytes = append(rse.bytes, makeCommand(cmdTypeEnd, 0))
	return rse.bytes
}

func (rse *RowSetEncoder) IsFinished() bool {
	return len(rse.bytes) > 0 && rse.bytes[len(rse.bytes)-1] == makeCommand(cmdTypeEnd, 0)
}

type RowSetSerializer struct {
	comp RowSetCompressor
}

func NewRowSetSerializer() RowSetSerializer {
	return RowSetSerializer{comp: NewRowSetCompressor()}
}

func (rss *RowSetSerializer) AddRow(key int64) {
	rss.comp.AddRow(key)
}

func (rss *RowSetSerializer) AddRowRange(start int64, end int64) {
	rss.comp.AddRowRange(start, end)
}

func (rss *RowSetSerializer) Finish() []byte {
	enc := NewRowSetEncoder()
	for _, offset := range rss.comp.offsets {
		enc.AddOffset(offset)
	}
	return enc.Finish()
}

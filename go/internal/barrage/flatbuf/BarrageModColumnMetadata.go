// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// Holds all of the rowset data structures for the column being modified.
type BarrageModColumnMetadata struct {
	_tab flatbuffers.Table
}

func GetRootAsBarrageModColumnMetadata(buf []byte, offset flatbuffers.UOffsetT) *BarrageModColumnMetadata {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &BarrageModColumnMetadata{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *BarrageModColumnMetadata) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *BarrageModColumnMetadata) Table() flatbuffers.Table {
	return rcv._tab
}

/// This is an encoded and compressed RowSet for this column (within the viewport) that were modified.
/// There is no notification for modifications outside of the viewport.
func (rcv *BarrageModColumnMetadata) ModifiedRows(j int) int8 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetInt8(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *BarrageModColumnMetadata) ModifiedRowsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// This is an encoded and compressed RowSet for this column (within the viewport) that were modified.
/// There is no notification for modifications outside of the viewport.
func (rcv *BarrageModColumnMetadata) MutateModifiedRows(j int, n int8) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateInt8(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func BarrageModColumnMetadataStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func BarrageModColumnMetadataAddModifiedRows(builder *flatbuffers.Builder, modifiedRows flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(modifiedRows), 0)
}
func BarrageModColumnMetadataStartModifiedRowsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func BarrageModColumnMetadataEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

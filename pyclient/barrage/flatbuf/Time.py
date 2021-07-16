# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Time type. The physical storage type depends on the unit
# - SECOND and MILLISECOND: 32 bits
# - MICROSECOND and NANOSECOND: 64 bits
class Time(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Time()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsTime(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # Time
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Time
    def Unit(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int16Flags, o + self._tab.Pos)
        return 1

    # Time
    def BitWidth(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 32

def Start(builder): builder.StartObject(2)
def TimeStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddUnit(builder, unit): builder.PrependInt16Slot(0, unit, 1)
def TimeAddUnit(builder, unit):
    """This method is deprecated. Please switch to AddUnit."""
    return AddUnit(builder, unit)
def AddBitWidth(builder, bitWidth): builder.PrependInt32Slot(1, bitWidth, 32)
def TimeAddBitWidth(builder, bitWidth):
    """This method is deprecated. Please switch to AddBitWidth."""
    return AddBitWidth(builder, bitWidth)
def End(builder): return builder.EndObject()
def TimeEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)
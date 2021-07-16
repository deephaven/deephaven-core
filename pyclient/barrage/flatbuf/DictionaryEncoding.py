# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class DictionaryEncoding(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = DictionaryEncoding()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsDictionaryEncoding(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # DictionaryEncoding
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # The known dictionary id in the application where this data is used. In
    # the file or streaming formats, the dictionary ids are found in the
    # DictionaryBatch messages
    # DictionaryEncoding
    def Id(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

    # The dictionary indices are constrained to be non-negative integers. If
    # this field is null, the indices must be signed int32. To maximize
    # cross-language compatibility and performance, implementations are
    # recommended to prefer signed integer types over unsigned integer types
    # and to avoid uint64 indices unless they are required by an application.
    # DictionaryEncoding
    def IndexType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from barrage.flatbuf.Int import Int
            obj = Int()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # By default, dictionaries are not ordered, or the order does not have
    # semantic meaning. In some statistical, applications, dictionary-encoding
    # is used to represent ordered categorical data, and we provide a way to
    # preserve that metadata here
    # DictionaryEncoding
    def IsOrdered(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return bool(self._tab.Get(flatbuffers.number_types.BoolFlags, o + self._tab.Pos))
        return False

    # DictionaryEncoding
    def DictionaryKind(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int16Flags, o + self._tab.Pos)
        return 0

def Start(builder): builder.StartObject(4)
def DictionaryEncodingStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddId(builder, id): builder.PrependInt64Slot(0, id, 0)
def DictionaryEncodingAddId(builder, id):
    """This method is deprecated. Please switch to AddId."""
    return AddId(builder, id)
def AddIndexType(builder, indexType): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(indexType), 0)
def DictionaryEncodingAddIndexType(builder, indexType):
    """This method is deprecated. Please switch to AddIndexType."""
    return AddIndexType(builder, indexType)
def AddIsOrdered(builder, isOrdered): builder.PrependBoolSlot(2, isOrdered, 0)
def DictionaryEncodingAddIsOrdered(builder, isOrdered):
    """This method is deprecated. Please switch to AddIsOrdered."""
    return AddIsOrdered(builder, isOrdered)
def AddDictionaryKind(builder, dictionaryKind): builder.PrependInt16Slot(3, dictionaryKind, 0)
def DictionaryEncodingAddDictionaryKind(builder, dictionaryKind):
    """This method is deprecated. Please switch to AddDictionaryKind."""
    return AddDictionaryKind(builder, dictionaryKind)
def End(builder): return builder.EndObject()
def DictionaryEncodingEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)
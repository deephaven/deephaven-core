# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Optional compression for the memory buffers constituting IPC message
# bodies. Intended for use with RecordBatch but could be used for other
# message types
class BodyCompression(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = BodyCompression()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsBodyCompression(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # BodyCompression
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Compressor library
    # BodyCompression
    def Codec(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int8Flags, o + self._tab.Pos)
        return 0

    # Indicates the way the record batch body was compressed
    # BodyCompression
    def Method(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int8Flags, o + self._tab.Pos)
        return 0

def Start(builder): builder.StartObject(2)
def BodyCompressionStart(builder):
    """This method is deprecated. Please switch to Start."""
    return Start(builder)
def AddCodec(builder, codec): builder.PrependInt8Slot(0, codec, 0)
def BodyCompressionAddCodec(builder, codec):
    """This method is deprecated. Please switch to AddCodec."""
    return AddCodec(builder, codec)
def AddMethod(builder, method): builder.PrependInt8Slot(1, method, 0)
def BodyCompressionAddMethod(builder, method):
    """This method is deprecated. Please switch to AddMethod."""
    return AddMethod(builder, method)
def End(builder): return builder.EndObject()
def BodyCompressionEnd(builder):
    """This method is deprecated. Please switch to End."""
    return End(builder)
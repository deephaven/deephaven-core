package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Buffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Message_generated.org.apache.arrow.flatbuf.RecordBatch",
    namespace = JsPackage.GLOBAL)
public class RecordBatch {
    public static native void addBuffers(Builder builder, double buffersOffset);

    public static native void addCompression(Builder builder, double compressionOffset);

    public static native void addLength(Builder builder, Long length);

    public static native void addNodes(Builder builder, double nodesOffset);

    public static native double endRecordBatch(Builder builder);

    public static native RecordBatch getRootAsRecordBatch(ByteBuffer bb, RecordBatch obj);

    public static native RecordBatch getRootAsRecordBatch(ByteBuffer bb);

    public static native RecordBatch getSizePrefixedRootAsRecordBatch(ByteBuffer bb,
        RecordBatch obj);

    public static native RecordBatch getSizePrefixedRootAsRecordBatch(ByteBuffer bb);

    public static native void startBuffersVector(Builder builder, double numElems);

    public static native void startNodesVector(Builder builder, double numElems);

    public static native void startRecordBatch(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native RecordBatch __init(double i, ByteBuffer bb);

    public native Buffer buffers(double index, Buffer obj);

    public native Buffer buffers(double index);

    public native double buffersLength();

    public native BodyCompression compression();

    public native BodyCompression compression(BodyCompression obj);

    public native Long length();

    public native FieldNode nodes(double index, FieldNode obj);

    public native FieldNode nodes(double index);

    public native double nodesLength();
}

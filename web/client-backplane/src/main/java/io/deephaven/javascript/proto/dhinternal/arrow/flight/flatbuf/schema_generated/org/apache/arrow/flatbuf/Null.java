package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Null",
    namespace = JsPackage.GLOBAL)
public class Null {
    public static native double createNull(Builder builder);

    public static native double endNull(Builder builder);

    public static native Null getRootAsNull(ByteBuffer bb, Null obj);

    public static native Null getRootAsNull(ByteBuffer bb);

    public static native Null getSizePrefixedRootAsNull(ByteBuffer bb, Null obj);

    public static native Null getSizePrefixedRootAsNull(ByteBuffer bb);

    public static native void startNull(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Null __init(double i, ByteBuffer bb);
}

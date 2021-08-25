package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Struct_",
    namespace = JsPackage.GLOBAL)
public class Struct_ {
    public static native double createStruct_(Builder builder);

    public static native double endStruct_(Builder builder);

    public static native Struct_ getRootAsStruct_(ByteBuffer bb, Struct_ obj);

    public static native Struct_ getRootAsStruct_(ByteBuffer bb);

    public static native Struct_ getSizePrefixedRootAsStruct_(ByteBuffer bb, Struct_ obj);

    public static native Struct_ getSizePrefixedRootAsStruct_(ByteBuffer bb);

    public static native void startStruct_(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Struct_ __init(double i, ByteBuffer bb);
}

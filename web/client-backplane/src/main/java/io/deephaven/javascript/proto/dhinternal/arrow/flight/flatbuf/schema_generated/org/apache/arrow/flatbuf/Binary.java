package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Binary",
    namespace = JsPackage.GLOBAL)
public class Binary {
    public static native double createBinary(Builder builder);

    public static native double endBinary(Builder builder);

    public static native Binary getRootAsBinary(ByteBuffer bb, Binary obj);

    public static native Binary getRootAsBinary(ByteBuffer bb);

    public static native Binary getSizePrefixedRootAsBinary(ByteBuffer bb, Binary obj);

    public static native Binary getSizePrefixedRootAsBinary(ByteBuffer bb);

    public static native void startBinary(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Binary __init(double i, ByteBuffer bb);
}

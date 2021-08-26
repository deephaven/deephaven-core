package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Utf8",
        namespace = JsPackage.GLOBAL)
public class Utf8 {
    public static native double createUtf8(Builder builder);

    public static native double endUtf8(Builder builder);

    public static native Utf8 getRootAsUtf8(ByteBuffer bb, Utf8 obj);

    public static native Utf8 getRootAsUtf8(ByteBuffer bb);

    public static native Utf8 getSizePrefixedRootAsUtf8(ByteBuffer bb, Utf8 obj);

    public static native Utf8 getSizePrefixedRootAsUtf8(ByteBuffer bb);

    public static native void startUtf8(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Utf8 __init(double i, ByteBuffer bb);
}

package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Bool",
        namespace = JsPackage.GLOBAL)
public class Bool {
    public static native double createBool(Builder builder);

    public static native double endBool(Builder builder);

    public static native Bool getRootAsBool(ByteBuffer bb, Bool obj);

    public static native Bool getRootAsBool(ByteBuffer bb);

    public static native Bool getSizePrefixedRootAsBool(ByteBuffer bb, Bool obj);

    public static native Bool getSizePrefixedRootAsBool(ByteBuffer bb);

    public static native void startBool(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Bool __init(double i, ByteBuffer bb);
}

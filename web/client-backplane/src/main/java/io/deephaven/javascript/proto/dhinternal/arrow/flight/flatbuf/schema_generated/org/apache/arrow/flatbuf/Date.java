package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Date",
    namespace = JsPackage.GLOBAL)
public class Date {
    public static native void addUnit(Builder builder, int unit);

    public static native double createDate(Builder builder, int unit);

    public static native double endDate(Builder builder);

    public static native Date getRootAsDate(ByteBuffer bb, Date obj);

    public static native Date getRootAsDate(ByteBuffer bb);

    public static native Date getSizePrefixedRootAsDate(ByteBuffer bb, Date obj);

    public static native Date getSizePrefixedRootAsDate(ByteBuffer bb);

    public static native void startDate(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Date __init(double i, ByteBuffer bb);

    public native int unit();
}

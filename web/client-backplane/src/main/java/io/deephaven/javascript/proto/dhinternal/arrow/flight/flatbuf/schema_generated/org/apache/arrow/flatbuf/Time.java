package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Time",
        namespace = JsPackage.GLOBAL)
public class Time {
    public static native void addBitWidth(Builder builder, double bitWidth);

    public static native void addUnit(Builder builder, int unit);

    public static native double createTime(Builder builder, int unit, double bitWidth);

    public static native double endTime(Builder builder);

    public static native Time getRootAsTime(ByteBuffer bb, Time obj);

    public static native Time getRootAsTime(ByteBuffer bb);

    public static native Time getSizePrefixedRootAsTime(ByteBuffer bb, Time obj);

    public static native Time getSizePrefixedRootAsTime(ByteBuffer bb);

    public static native void startTime(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Time __init(double i, ByteBuffer bb);

    public native double bitWidth();

    public native int unit();
}

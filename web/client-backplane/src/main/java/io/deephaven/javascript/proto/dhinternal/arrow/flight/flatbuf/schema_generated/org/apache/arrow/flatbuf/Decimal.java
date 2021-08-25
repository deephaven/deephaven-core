package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Decimal",
        namespace = JsPackage.GLOBAL)
public class Decimal {
    public static native void addBitWidth(Builder builder, double bitWidth);

    public static native void addPrecision(Builder builder, double precision);

    public static native void addScale(Builder builder, double scale);

    public static native double createDecimal(
            Builder builder, double precision, double scale, double bitWidth);

    public static native double endDecimal(Builder builder);

    public static native Decimal getRootAsDecimal(ByteBuffer bb, Decimal obj);

    public static native Decimal getRootAsDecimal(ByteBuffer bb);

    public static native Decimal getSizePrefixedRootAsDecimal(ByteBuffer bb, Decimal obj);

    public static native Decimal getSizePrefixedRootAsDecimal(ByteBuffer bb);

    public static native void startDecimal(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Decimal __init(double i, ByteBuffer bb);

    public native double bitWidth();

    public native double precision();

    public native double scale();
}

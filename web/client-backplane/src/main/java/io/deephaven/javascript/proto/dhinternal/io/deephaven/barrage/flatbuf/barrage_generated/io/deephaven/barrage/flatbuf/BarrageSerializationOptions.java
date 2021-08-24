package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarrageSerializationOptions",
    namespace = JsPackage.GLOBAL)
public class BarrageSerializationOptions {
    public static native void addColumnConversionMode(
        Builder builder, int columnConversionMode);

    public static native void addUseDeephavenNulls(Builder builder, boolean useDeephavenNulls);

    public static native double createBarrageSerializationOptions(
        Builder builder, int columnConversionMode, boolean useDeephavenNulls);

    public static native double endBarrageSerializationOptions(Builder builder);

    public static native BarrageSerializationOptions getRootAsBarrageSerializationOptions(
        ByteBuffer bb, BarrageSerializationOptions obj);

    public static native BarrageSerializationOptions getRootAsBarrageSerializationOptions(
        ByteBuffer bb);

    public static native BarrageSerializationOptions getSizePrefixedRootAsBarrageSerializationOptions(
        ByteBuffer bb, BarrageSerializationOptions obj);

    public static native BarrageSerializationOptions getSizePrefixedRootAsBarrageSerializationOptions(
        ByteBuffer bb);

    public static native void startBarrageSerializationOptions(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarrageSerializationOptions __init(double i, ByteBuffer bb);

    public native int columnConversionMode();

    public native boolean useDeephavenNulls();
}

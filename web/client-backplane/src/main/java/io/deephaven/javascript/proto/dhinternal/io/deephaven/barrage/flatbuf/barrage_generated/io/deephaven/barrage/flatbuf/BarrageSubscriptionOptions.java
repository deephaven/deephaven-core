package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions",
        namespace = JsPackage.GLOBAL)
public class BarrageSubscriptionOptions {
    public static native void addBatchSize(Builder builder, double batchSize);

    public static native void addColumnConversionMode(
            Builder builder, int columnConversionMode);

    public static native void addMinUpdateIntervalMs(Builder builder, double minUpdateIntervalMs);

    public static native void addUseDeephavenNulls(Builder builder, boolean useDeephavenNulls);

    public static native double createBarrageSubscriptionOptions(
            Builder builder,
            int columnConversionMode,
            boolean useDeephavenNulls,
            double minUpdateIntervalMs,
            double batchSize);

    public static native double endBarrageSubscriptionOptions(Builder builder);

    public static native BarrageSubscriptionOptions getRootAsBarrageSubscriptionOptions(
            ByteBuffer bb, BarrageSubscriptionOptions obj);

    public static native BarrageSubscriptionOptions getRootAsBarrageSubscriptionOptions(
            ByteBuffer bb);

    public static native BarrageSubscriptionOptions getSizePrefixedRootAsBarrageSubscriptionOptions(
            ByteBuffer bb, BarrageSubscriptionOptions obj);

    public static native BarrageSubscriptionOptions getSizePrefixedRootAsBarrageSubscriptionOptions(
            ByteBuffer bb);

    public static native void startBarrageSubscriptionOptions(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarrageSubscriptionOptions __init(double i, ByteBuffer bb);

    public native double batchSize();

    public native int columnConversionMode();

    public native double minUpdateIntervalMs();

    public native boolean useDeephavenNulls();
}

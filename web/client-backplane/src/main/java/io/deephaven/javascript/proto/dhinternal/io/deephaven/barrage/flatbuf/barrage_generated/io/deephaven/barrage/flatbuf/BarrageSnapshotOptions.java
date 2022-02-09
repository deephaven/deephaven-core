package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarrageSnapshotOptions",
        namespace = JsPackage.GLOBAL)
public class BarrageSnapshotOptions {
    public static native void addBatchSize(Builder builder, double batchSize);

    public static native void addColumnConversionMode(
            Builder builder, int columnConversionMode);

    public static native void addUseDeephavenNulls(Builder builder, boolean useDeephavenNulls);

    public static native double createBarrageSnapshotOptions(
            Builder builder,
            int columnConversionMode,
            boolean useDeephavenNulls,
            double batchSize);

    public static native double endBarrageSnapshotOptions(Builder builder);

    public static native BarrageSnapshotOptions getRootAsBarrageSnapshotOptions(
            ByteBuffer bb, BarrageSnapshotOptions obj);

    public static native BarrageSnapshotOptions getRootAsBarrageSnapshotOptions(ByteBuffer bb);

    public static native BarrageSnapshotOptions getSizePrefixedRootAsBarrageSnapshotOptions(
            ByteBuffer bb, BarrageSnapshotOptions obj);

    public static native BarrageSnapshotOptions getSizePrefixedRootAsBarrageSnapshotOptions(
            ByteBuffer bb);

    public static native void startBarrageSnapshotOptions(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarrageSnapshotOptions __init(double i, ByteBuffer bb);

    public native double batchSize();

    public native int columnConversionMode();

    public native boolean useDeephavenNulls();
}

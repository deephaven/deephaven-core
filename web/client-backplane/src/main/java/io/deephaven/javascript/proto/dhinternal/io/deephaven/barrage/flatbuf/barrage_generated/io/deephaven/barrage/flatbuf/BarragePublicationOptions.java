package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarragePublicationOptions",
        namespace = JsPackage.GLOBAL)
public class BarragePublicationOptions {
    public static native void addUseDeephavenNulls(Builder builder, boolean useDeephavenNulls);

    public static native double createBarragePublicationOptions(
            Builder builder, boolean useDeephavenNulls);

    public static native double endBarragePublicationOptions(Builder builder);

    public static native BarragePublicationOptions getRootAsBarragePublicationOptions(
            ByteBuffer bb, BarragePublicationOptions obj);

    public static native BarragePublicationOptions getRootAsBarragePublicationOptions(ByteBuffer bb);

    public static native BarragePublicationOptions getSizePrefixedRootAsBarragePublicationOptions(
            ByteBuffer bb, BarragePublicationOptions obj);

    public static native BarragePublicationOptions getSizePrefixedRootAsBarragePublicationOptions(
            ByteBuffer bb);

    public static native void startBarragePublicationOptions(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarragePublicationOptions __init(double i, ByteBuffer bb);

    public native boolean useDeephavenNulls();
}

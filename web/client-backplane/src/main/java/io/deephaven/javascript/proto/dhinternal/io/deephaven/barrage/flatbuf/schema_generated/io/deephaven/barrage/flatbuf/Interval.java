package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.Interval",
    namespace = JsPackage.GLOBAL)
public class Interval {
  public static native void addUnit(Builder builder, int unit);

  public static native double createInterval(Builder builder, int unit);

  public static native double endInterval(Builder builder);

  public static native Interval getRootAsInterval(ByteBuffer bb, Interval obj);

  public static native Interval getRootAsInterval(ByteBuffer bb);

  public static native Interval getSizePrefixedRootAsInterval(ByteBuffer bb, Interval obj);

  public static native Interval getSizePrefixedRootAsInterval(ByteBuffer bb);

  public static native void startInterval(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Interval __init(double i, ByteBuffer bb);

  public native int unit();
}

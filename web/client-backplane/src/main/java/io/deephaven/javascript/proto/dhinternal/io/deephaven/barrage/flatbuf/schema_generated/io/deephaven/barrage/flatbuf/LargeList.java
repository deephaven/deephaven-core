package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.LargeList",
    namespace = JsPackage.GLOBAL)
public class LargeList {
  public static native double createLargeList(Builder builder);

  public static native double endLargeList(Builder builder);

  public static native LargeList getRootAsLargeList(ByteBuffer bb, LargeList obj);

  public static native LargeList getRootAsLargeList(ByteBuffer bb);

  public static native LargeList getSizePrefixedRootAsLargeList(ByteBuffer bb, LargeList obj);

  public static native LargeList getSizePrefixedRootAsLargeList(ByteBuffer bb);

  public static native void startLargeList(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native LargeList __init(double i, ByteBuffer bb);
}

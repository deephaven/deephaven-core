package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.FixedSizeList",
    namespace = JsPackage.GLOBAL)
public class FixedSizeList {
  public static native void addListSize(Builder builder, double listSize);

  public static native double createFixedSizeList(Builder builder, double listSize);

  public static native double endFixedSizeList(Builder builder);

  public static native FixedSizeList getRootAsFixedSizeList(ByteBuffer bb, FixedSizeList obj);

  public static native FixedSizeList getRootAsFixedSizeList(ByteBuffer bb);

  public static native FixedSizeList getSizePrefixedRootAsFixedSizeList(
      ByteBuffer bb, FixedSizeList obj);

  public static native FixedSizeList getSizePrefixedRootAsFixedSizeList(ByteBuffer bb);

  public static native void startFixedSizeList(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native FixedSizeList __init(double i, ByteBuffer bb);

  public native double listSize();
}

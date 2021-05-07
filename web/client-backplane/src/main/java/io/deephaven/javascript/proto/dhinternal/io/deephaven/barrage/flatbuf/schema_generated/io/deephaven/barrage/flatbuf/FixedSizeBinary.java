package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.FixedSizeBinary",
    namespace = JsPackage.GLOBAL)
public class FixedSizeBinary {
  public static native void addByteWidth(Builder builder, double byteWidth);

  public static native double createFixedSizeBinary(Builder builder, double byteWidth);

  public static native double endFixedSizeBinary(Builder builder);

  public static native FixedSizeBinary getRootAsFixedSizeBinary(ByteBuffer bb, FixedSizeBinary obj);

  public static native FixedSizeBinary getRootAsFixedSizeBinary(ByteBuffer bb);

  public static native FixedSizeBinary getSizePrefixedRootAsFixedSizeBinary(
      ByteBuffer bb, FixedSizeBinary obj);

  public static native FixedSizeBinary getSizePrefixedRootAsFixedSizeBinary(ByteBuffer bb);

  public static native void startFixedSizeBinary(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native FixedSizeBinary __init(double i, ByteBuffer bb);

  public native double byteWidth();
}

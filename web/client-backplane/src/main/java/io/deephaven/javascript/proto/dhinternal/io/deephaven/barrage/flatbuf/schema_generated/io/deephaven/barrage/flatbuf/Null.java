package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.Null",
    namespace = JsPackage.GLOBAL)
public class Null {
  public static native double createNull(Builder builder);

  public static native double endNull(Builder builder);

  public static native Null getRootAsNull(ByteBuffer bb, Null obj);

  public static native Null getRootAsNull(ByteBuffer bb);

  public static native Null getSizePrefixedRootAsNull(ByteBuffer bb, Null obj);

  public static native Null getSizePrefixedRootAsNull(ByteBuffer bb);

  public static native void startNull(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Null __init(double i, ByteBuffer bb);
}

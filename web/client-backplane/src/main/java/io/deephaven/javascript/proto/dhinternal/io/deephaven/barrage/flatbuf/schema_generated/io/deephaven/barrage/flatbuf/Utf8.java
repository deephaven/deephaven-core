package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.Utf8",
    namespace = JsPackage.GLOBAL)
public class Utf8 {
  public static native double createUtf8(Builder builder);

  public static native double endUtf8(Builder builder);

  public static native Utf8 getRootAsUtf8(ByteBuffer bb, Utf8 obj);

  public static native Utf8 getRootAsUtf8(ByteBuffer bb);

  public static native Utf8 getSizePrefixedRootAsUtf8(ByteBuffer bb, Utf8 obj);

  public static native Utf8 getSizePrefixedRootAsUtf8(ByteBuffer bb);

  public static native void startUtf8(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Utf8 __init(double i, ByteBuffer bb);
}

package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.Bool",
    namespace = JsPackage.GLOBAL)
public class Bool {
  public static native double createBool(Builder builder);

  public static native double endBool(Builder builder);

  public static native Bool getRootAsBool(ByteBuffer bb, Bool obj);

  public static native Bool getRootAsBool(ByteBuffer bb);

  public static native Bool getSizePrefixedRootAsBool(ByteBuffer bb, Bool obj);

  public static native Bool getSizePrefixedRootAsBool(ByteBuffer bb);

  public static native void startBool(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Bool __init(double i, ByteBuffer bb);
}

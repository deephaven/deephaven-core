package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.Binary",
    namespace = JsPackage.GLOBAL)
public class Binary {
  public static native double createBinary(Builder builder);

  public static native double endBinary(Builder builder);

  public static native Binary getRootAsBinary(ByteBuffer bb, Binary obj);

  public static native Binary getRootAsBinary(ByteBuffer bb);

  public static native Binary getSizePrefixedRootAsBinary(ByteBuffer bb, Binary obj);

  public static native Binary getSizePrefixedRootAsBinary(ByteBuffer bb);

  public static native void startBinary(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Binary __init(double i, ByteBuffer bb);
}

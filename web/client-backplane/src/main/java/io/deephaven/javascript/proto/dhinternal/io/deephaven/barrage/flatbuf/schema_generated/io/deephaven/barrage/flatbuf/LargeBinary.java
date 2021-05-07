package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.LargeBinary",
    namespace = JsPackage.GLOBAL)
public class LargeBinary {
  public static native double createLargeBinary(Builder builder);

  public static native double endLargeBinary(Builder builder);

  public static native LargeBinary getRootAsLargeBinary(ByteBuffer bb, LargeBinary obj);

  public static native LargeBinary getRootAsLargeBinary(ByteBuffer bb);

  public static native LargeBinary getSizePrefixedRootAsLargeBinary(ByteBuffer bb, LargeBinary obj);

  public static native LargeBinary getSizePrefixedRootAsLargeBinary(ByteBuffer bb);

  public static native void startLargeBinary(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native LargeBinary __init(double i, ByteBuffer bb);
}

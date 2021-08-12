package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.LargeUtf8",
    namespace = JsPackage.GLOBAL)
public class LargeUtf8 {
  public static native double createLargeUtf8(Builder builder);

  public static native double endLargeUtf8(Builder builder);

  public static native LargeUtf8 getRootAsLargeUtf8(ByteBuffer bb, LargeUtf8 obj);

  public static native LargeUtf8 getRootAsLargeUtf8(ByteBuffer bb);

  public static native LargeUtf8 getSizePrefixedRootAsLargeUtf8(ByteBuffer bb, LargeUtf8 obj);

  public static native LargeUtf8 getSizePrefixedRootAsLargeUtf8(ByteBuffer bb);

  public static native void startLargeUtf8(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native LargeUtf8 __init(double i, ByteBuffer bb);
}

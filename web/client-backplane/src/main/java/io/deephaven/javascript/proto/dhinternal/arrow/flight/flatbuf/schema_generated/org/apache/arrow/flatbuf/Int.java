package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Int",
    namespace = JsPackage.GLOBAL)
public class Int {
  public static native void addBitWidth(Builder builder, double bitWidth);

  public static native void addIsSigned(Builder builder, boolean isSigned);

  public static native double createInt(Builder builder, double bitWidth, boolean isSigned);

  public static native double endInt(Builder builder);

  public static native Int getRootAsInt(ByteBuffer bb, Int obj);

  public static native Int getRootAsInt(ByteBuffer bb);

  public static native Int getSizePrefixedRootAsInt(ByteBuffer bb, Int obj);

  public static native Int getSizePrefixedRootAsInt(ByteBuffer bb);

  public static native void startInt(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Int __init(double i, ByteBuffer bb);

  public native double bitWidth();

  public native boolean isSigned();
}

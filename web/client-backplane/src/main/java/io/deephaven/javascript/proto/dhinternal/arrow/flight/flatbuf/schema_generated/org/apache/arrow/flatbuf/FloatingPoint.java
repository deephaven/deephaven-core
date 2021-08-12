package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.FloatingPoint",
    namespace = JsPackage.GLOBAL)
public class FloatingPoint {
  public static native void addPrecision(Builder builder, int precision);

  public static native double createFloatingPoint(Builder builder, int precision);

  public static native double endFloatingPoint(Builder builder);

  public static native FloatingPoint getRootAsFloatingPoint(ByteBuffer bb, FloatingPoint obj);

  public static native FloatingPoint getRootAsFloatingPoint(ByteBuffer bb);

  public static native FloatingPoint getSizePrefixedRootAsFloatingPoint(
      ByteBuffer bb, FloatingPoint obj);

  public static native FloatingPoint getSizePrefixedRootAsFloatingPoint(ByteBuffer bb);

  public static native void startFloatingPoint(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native FloatingPoint __init(double i, ByteBuffer bb);

  public native int precision();
}

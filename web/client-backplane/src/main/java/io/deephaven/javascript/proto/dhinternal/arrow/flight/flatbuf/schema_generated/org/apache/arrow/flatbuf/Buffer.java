package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Buffer",
    namespace = JsPackage.GLOBAL)
public class Buffer {
  public static native double createBuffer(Builder builder, Long offset, Long length);

  public static native double sizeOf();

  public ByteBuffer bb;
  public double bb_pos;

  public native Buffer __init(double i, ByteBuffer bb);

  public native Long length();

  public native Long offset();
}

package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.Buffer",
    namespace = JsPackage.GLOBAL)
public class Buffer {
  public static native double createBuffer(Builder builder, Long offset, Long length);

  public ByteBuffer bb;
  public double bb_pos;

  public native Buffer __init(double i, ByteBuffer bb);

  public native Long length();

  public native Long offset();
}

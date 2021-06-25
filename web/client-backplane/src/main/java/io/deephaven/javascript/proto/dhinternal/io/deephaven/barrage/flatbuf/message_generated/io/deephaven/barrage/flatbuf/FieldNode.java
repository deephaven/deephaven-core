package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.message_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Message_generated.io.deephaven.barrage.flatbuf.FieldNode",
    namespace = JsPackage.GLOBAL)
public class FieldNode {
  public static native double createFieldNode(Builder builder, Long length, Long null_count);

  public ByteBuffer bb;
  public double bb_pos;

  public native FieldNode __init(double i, ByteBuffer bb);

  public native Long length();

  public native Long nullCount();
}

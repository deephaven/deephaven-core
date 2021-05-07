package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.List",
    namespace = JsPackage.GLOBAL)
public class List {
  public static native double createList(Builder builder);

  public static native double endList(Builder builder);

  public static native List getRootAsList(ByteBuffer bb, List obj);

  public static native List getRootAsList(ByteBuffer bb);

  public static native List getSizePrefixedRootAsList(ByteBuffer bb, List obj);

  public static native List getSizePrefixedRootAsList(ByteBuffer bb);

  public static native void startList(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native List __init(double i, ByteBuffer bb);
}

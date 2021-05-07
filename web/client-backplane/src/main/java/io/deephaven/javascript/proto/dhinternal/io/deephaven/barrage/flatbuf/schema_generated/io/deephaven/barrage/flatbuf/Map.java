package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Schema_generated.io.deephaven.barrage.flatbuf.Map",
    namespace = JsPackage.GLOBAL)
public class Map {
  public static native void addKeysSorted(Builder builder, boolean keysSorted);

  public static native double createMap(Builder builder, boolean keysSorted);

  public static native double endMap(Builder builder);

  public static native Map getRootAsMap(ByteBuffer bb, Map obj);

  public static native Map getRootAsMap(ByteBuffer bb);

  public static native Map getSizePrefixedRootAsMap(ByteBuffer bb, Map obj);

  public static native Map getSizePrefixedRootAsMap(ByteBuffer bb);

  public static native void startMap(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Map __init(double i, ByteBuffer bb);

  public native boolean keysSorted();
}

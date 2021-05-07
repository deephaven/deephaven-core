package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Buffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Int;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.SparseTensor_generated.io.deephaven.barrage.flatbuf.SparseTensorIndexCOO",
    namespace = JsPackage.GLOBAL)
public class SparseTensorIndexCOO {
  public static native void addIndicesBuffer(Builder builder, double indicesBufferOffset);

  public static native void addIndicesStrides(Builder builder, double indicesStridesOffset);

  public static native void addIndicesType(Builder builder, double indicesTypeOffset);

  public static native void addIsCanonical(Builder builder, boolean isCanonical);

  public static native double createIndicesStridesVector(Builder builder, JsArray<Object> data);

  @JsOverlay
  public static final double createIndicesStridesVector(Builder builder, Object[] data) {
    return createIndicesStridesVector(builder, Js.<JsArray<Object>>uncheckedCast(data));
  }

  public static native double endSparseTensorIndexCOO(Builder builder);

  public static native SparseTensorIndexCOO getRootAsSparseTensorIndexCOO(
      ByteBuffer bb, SparseTensorIndexCOO obj);

  public static native SparseTensorIndexCOO getRootAsSparseTensorIndexCOO(ByteBuffer bb);

  public static native SparseTensorIndexCOO getSizePrefixedRootAsSparseTensorIndexCOO(
      ByteBuffer bb, SparseTensorIndexCOO obj);

  public static native SparseTensorIndexCOO getSizePrefixedRootAsSparseTensorIndexCOO(
      ByteBuffer bb);

  public static native void startIndicesStridesVector(Builder builder, double numElems);

  public static native void startSparseTensorIndexCOO(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native SparseTensorIndexCOO __init(double i, ByteBuffer bb);

  public native Buffer indicesBuffer();

  public native Buffer indicesBuffer(Buffer obj);

  public native Long indicesStrides(double index);

  public native double indicesStridesLength();

  public native Int indicesType();

  public native Int indicesType(Int obj);

  public native boolean isCanonical();
}

package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Buffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Int;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.SparseTensor_generated.io.deephaven.barrage.flatbuf.SparseMatrixIndexCSX",
    namespace = JsPackage.GLOBAL)
public class SparseMatrixIndexCSX {
  public static native void addCompressedAxis(
      Builder builder, int compressedAxis);

  public static native void addIndicesBuffer(Builder builder, double indicesBufferOffset);

  public static native void addIndicesType(Builder builder, double indicesTypeOffset);

  public static native void addIndptrBuffer(Builder builder, double indptrBufferOffset);

  public static native void addIndptrType(Builder builder, double indptrTypeOffset);

  public static native double endSparseMatrixIndexCSX(Builder builder);

  public static native SparseMatrixIndexCSX getRootAsSparseMatrixIndexCSX(
      ByteBuffer bb, SparseMatrixIndexCSX obj);

  public static native SparseMatrixIndexCSX getRootAsSparseMatrixIndexCSX(ByteBuffer bb);

  public static native SparseMatrixIndexCSX getSizePrefixedRootAsSparseMatrixIndexCSX(
      ByteBuffer bb, SparseMatrixIndexCSX obj);

  public static native SparseMatrixIndexCSX getSizePrefixedRootAsSparseMatrixIndexCSX(
      ByteBuffer bb);

  public static native void startSparseMatrixIndexCSX(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native SparseMatrixIndexCSX __init(double i, ByteBuffer bb);

  public native int compressedAxis();

  public native Buffer indicesBuffer();

  public native Buffer indicesBuffer(Buffer obj);

  public native Int indicesType();

  public native Int indicesType(Int obj);

  public native Buffer indptrBuffer();

  public native Buffer indptrBuffer(Buffer obj);

  public native Int indptrType();

  public native Int indptrType(Int obj);
}

package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Buffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.tensor_generated.io.deephaven.barrage.flatbuf.TensorDim;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.SparseTensor_generated.io.deephaven.barrage.flatbuf.SparseTensor",
    namespace = JsPackage.GLOBAL)
public class SparseTensor {
  public static native void addData(Builder builder, double dataOffset);

  public static native void addNonZeroLength(Builder builder, Long nonZeroLength);

  public static native void addShape(Builder builder, double shapeOffset);

  public static native void addSparseIndex(Builder builder, double sparseIndexOffset);

  public static native void addSparseIndexType(Builder builder, int sparseIndexType);

  public static native void addType(Builder builder, double typeOffset);

  public static native void addTypeType(Builder builder, int typeType);

  public static native double createShapeVector(Builder builder, JsArray<Double> data);

  @JsOverlay
  public static final double createShapeVector(Builder builder, double[] data) {
    return createShapeVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double endSparseTensor(Builder builder);

  public static native void finishSizePrefixedSparseTensorBuffer(Builder builder, double offset);

  public static native void finishSparseTensorBuffer(Builder builder, double offset);

  public static native SparseTensor getRootAsSparseTensor(ByteBuffer bb, SparseTensor obj);

  public static native SparseTensor getRootAsSparseTensor(ByteBuffer bb);

  public static native SparseTensor getSizePrefixedRootAsSparseTensor(
      ByteBuffer bb, SparseTensor obj);

  public static native SparseTensor getSizePrefixedRootAsSparseTensor(ByteBuffer bb);

  public static native void startShapeVector(Builder builder, double numElems);

  public static native void startSparseTensor(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native SparseTensor __init(double i, ByteBuffer bb);

  public native Buffer data();

  public native Buffer data(Buffer obj);

  public native Long nonZeroLength();

  public native TensorDim shape(double index, TensorDim obj);

  public native TensorDim shape(double index);

  public native double shapeLength();

  public native <T> T sparseIndex(T obj);

  public native int sparseIndexType();

  public native <T> T type(T obj);

  public native int typeType();
}

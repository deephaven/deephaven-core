package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.tensor_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Buffer;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Tensor_generated.io.deephaven.barrage.flatbuf.Tensor",
    namespace = JsPackage.GLOBAL)
public class Tensor {
  public static native void addData(Builder builder, double dataOffset);

  public static native void addShape(Builder builder, double shapeOffset);

  public static native void addStrides(Builder builder, double stridesOffset);

  public static native void addType(Builder builder, double typeOffset);

  public static native void addTypeType(Builder builder, int typeType);

  public static native double createShapeVector(Builder builder, JsArray<Double> data);

  @JsOverlay
  public static final double createShapeVector(Builder builder, double[] data) {
    return createShapeVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double createStridesVector(Builder builder, JsArray<Object> data);

  @JsOverlay
  public static final double createStridesVector(Builder builder, Object[] data) {
    return createStridesVector(builder, Js.<JsArray<Object>>uncheckedCast(data));
  }

  public static native double endTensor(Builder builder);

  public static native void finishSizePrefixedTensorBuffer(Builder builder, double offset);

  public static native void finishTensorBuffer(Builder builder, double offset);

  public static native Tensor getRootAsTensor(ByteBuffer bb, Tensor obj);

  public static native Tensor getRootAsTensor(ByteBuffer bb);

  public static native Tensor getSizePrefixedRootAsTensor(ByteBuffer bb, Tensor obj);

  public static native Tensor getSizePrefixedRootAsTensor(ByteBuffer bb);

  public static native void startShapeVector(Builder builder, double numElems);

  public static native void startStridesVector(Builder builder, double numElems);

  public static native void startTensor(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Tensor __init(double i, ByteBuffer bb);

  public native Buffer data();

  public native Buffer data(Buffer obj);

  public native TensorDim shape(double index, TensorDim obj);

  public native TensorDim shape(double index);

  public native double shapeLength();

  public native Long strides(double index);

  public native double stridesLength();

  public native <T> T type(T obj);

  public native int typeType();
}

package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.Int32Array;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Buffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Int;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.SparseTensor_generated.io.deephaven.barrage.flatbuf.SparseTensorIndexCSF",
    namespace = JsPackage.GLOBAL)
public class SparseTensorIndexCSF {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface CreateAxisOrderVectorDataUnionType {
    @JsOverlay
    static SparseTensorIndexCSF.CreateAxisOrderVectorDataUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default Int32Array asInt32Array() {
      return Js.cast(this);
    }

    @JsOverlay
    default JsArray<Double> asJsArray() {
      return Js.cast(this);
    }

    @JsOverlay
    default Uint8Array asUint8Array() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isInt32Array() {
      return (Object) this instanceof Int32Array;
    }

    @JsOverlay
    default boolean isJsArray() {
      return (Object) this instanceof JsArray;
    }

    @JsOverlay
    default boolean isUint8Array() {
      return (Object) this instanceof Uint8Array;
    }
  }

  public static native void addAxisOrder(Builder builder, double axisOrderOffset);

  public static native void addIndicesBuffers(Builder builder, double indicesBuffersOffset);

  public static native void addIndicesType(Builder builder, double indicesTypeOffset);

  public static native void addIndptrBuffers(Builder builder, double indptrBuffersOffset);

  public static native void addIndptrType(Builder builder, double indptrTypeOffset);

  @Deprecated
  public static native double createAxisOrderVector(
      Builder builder, SparseTensorIndexCSF.CreateAxisOrderVectorDataUnionType data);

  @JsOverlay
  @Deprecated
  public static final double createAxisOrderVector(Builder builder, Int32Array data) {
    return createAxisOrderVector(
        builder, Js.<SparseTensorIndexCSF.CreateAxisOrderVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createAxisOrderVector(Builder builder, JsArray<Double> data) {
    return createAxisOrderVector(
        builder, Js.<SparseTensorIndexCSF.CreateAxisOrderVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createAxisOrderVector(Builder builder, Uint8Array data) {
    return createAxisOrderVector(
        builder, Js.<SparseTensorIndexCSF.CreateAxisOrderVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createAxisOrderVector(Builder builder, double[] data) {
    return createAxisOrderVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double endSparseTensorIndexCSF(Builder builder);

  public static native SparseTensorIndexCSF getRootAsSparseTensorIndexCSF(
      ByteBuffer bb, SparseTensorIndexCSF obj);

  public static native SparseTensorIndexCSF getRootAsSparseTensorIndexCSF(ByteBuffer bb);

  public static native SparseTensorIndexCSF getSizePrefixedRootAsSparseTensorIndexCSF(
      ByteBuffer bb, SparseTensorIndexCSF obj);

  public static native SparseTensorIndexCSF getSizePrefixedRootAsSparseTensorIndexCSF(
      ByteBuffer bb);

  public static native void startAxisOrderVector(Builder builder, double numElems);

  public static native void startIndicesBuffersVector(Builder builder, double numElems);

  public static native void startIndptrBuffersVector(Builder builder, double numElems);

  public static native void startSparseTensorIndexCSF(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native SparseTensorIndexCSF __init(double i, ByteBuffer bb);

  public native double axisOrder(double index);

  public native Int32Array axisOrderArray();

  public native double axisOrderLength();

  public native Buffer indicesBuffers(double index, Buffer obj);

  public native Buffer indicesBuffers(double index);

  public native double indicesBuffersLength();

  public native Int indicesType();

  public native Int indicesType(Int obj);

  public native Buffer indptrBuffers(double index, Buffer obj);

  public native Buffer indptrBuffers(double index);

  public native double indptrBuffersLength();

  public native Int indptrType();

  public native Int indptrType(Int obj);
}

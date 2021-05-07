package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.Int8Array;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarrageFieldNode",
    namespace = JsPackage.GLOBAL)
public class BarrageFieldNode {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface CreateIncludedRowsVectorDataUnionType {
    @JsOverlay
    static BarrageFieldNode.CreateIncludedRowsVectorDataUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default Int8Array asInt8Array() {
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
    default boolean isInt8Array() {
      return (Object) this instanceof Int8Array;
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

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface CreateModifiedRowsVectorDataUnionType {
    @JsOverlay
    static BarrageFieldNode.CreateModifiedRowsVectorDataUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default Int8Array asInt8Array() {
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
    default boolean isInt8Array() {
      return (Object) this instanceof Int8Array;
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

  public static native void addIncludedRows(Builder builder, double includedRowsOffset);

  public static native void addLength(Builder builder, Long length);

  public static native void addModifiedRows(Builder builder, double modifiedRowsOffset);

  public static native void addNullCount(Builder builder, Long nullCount);

  public static native double createBarrageFieldNode(
      Builder builder,
      Long length,
      Long nullCount,
      double modifiedRowsOffset,
      double includedRowsOffset);

  @Deprecated
  public static native double createIncludedRowsVector(
      Builder builder, BarrageFieldNode.CreateIncludedRowsVectorDataUnionType data);

  @JsOverlay
  @Deprecated
  public static final double createIncludedRowsVector(Builder builder, Int8Array data) {
    return createIncludedRowsVector(
        builder, Js.<BarrageFieldNode.CreateIncludedRowsVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createIncludedRowsVector(Builder builder, JsArray<Double> data) {
    return createIncludedRowsVector(
        builder, Js.<BarrageFieldNode.CreateIncludedRowsVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createIncludedRowsVector(Builder builder, Uint8Array data) {
    return createIncludedRowsVector(
        builder, Js.<BarrageFieldNode.CreateIncludedRowsVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createIncludedRowsVector(Builder builder, double[] data) {
    return createIncludedRowsVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  @Deprecated
  public static native double createModifiedRowsVector(
      Builder builder, BarrageFieldNode.CreateModifiedRowsVectorDataUnionType data);

  @JsOverlay
  @Deprecated
  public static final double createModifiedRowsVector(Builder builder, Int8Array data) {
    return createModifiedRowsVector(
        builder, Js.<BarrageFieldNode.CreateModifiedRowsVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createModifiedRowsVector(Builder builder, JsArray<Double> data) {
    return createModifiedRowsVector(
        builder, Js.<BarrageFieldNode.CreateModifiedRowsVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createModifiedRowsVector(Builder builder, Uint8Array data) {
    return createModifiedRowsVector(
        builder, Js.<BarrageFieldNode.CreateModifiedRowsVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createModifiedRowsVector(Builder builder, double[] data) {
    return createModifiedRowsVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double endBarrageFieldNode(Builder builder);

  public static native BarrageFieldNode getRootAsBarrageFieldNode(
      ByteBuffer bb, BarrageFieldNode obj);

  public static native BarrageFieldNode getRootAsBarrageFieldNode(ByteBuffer bb);

  public static native BarrageFieldNode getSizePrefixedRootAsBarrageFieldNode(
      ByteBuffer bb, BarrageFieldNode obj);

  public static native BarrageFieldNode getSizePrefixedRootAsBarrageFieldNode(ByteBuffer bb);

  public static native void startBarrageFieldNode(Builder builder);

  public static native void startIncludedRowsVector(Builder builder, double numElems);

  public static native void startModifiedRowsVector(Builder builder, double numElems);

  public ByteBuffer bb;
  public double bb_pos;

  public native BarrageFieldNode __init(double i, ByteBuffer bb);

  public native double includedRows(double index);

  public native Int8Array includedRowsArray();

  public native double includedRowsLength();

  public native Long length();

  public native double modifiedRows(double index);

  public native Int8Array modifiedRowsArray();

  public native double modifiedRowsLength();

  public native Long nullCount();
}

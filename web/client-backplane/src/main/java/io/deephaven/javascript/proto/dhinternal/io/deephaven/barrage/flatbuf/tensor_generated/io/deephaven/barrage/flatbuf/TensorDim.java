package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.tensor_generated.io.deephaven.barrage.flatbuf;

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
        "dhinternal.io.deephaven.barrage.flatbuf.Tensor_generated.io.deephaven.barrage.flatbuf.TensorDim",
    namespace = JsPackage.GLOBAL)
public class TensorDim {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface NameUnionType {
    @JsOverlay
    static TensorDim.NameUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default String asString() {
      return Js.asString(this);
    }

    @JsOverlay
    default Uint8Array asUint8Array() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isString() {
      return (Object) this instanceof String;
    }

    @JsOverlay
    default boolean isUint8Array() {
      return (Object) this instanceof Uint8Array;
    }
  }

  public static native void addName(Builder builder, double nameOffset);

  public static native void addSize(Builder builder, Long size);

  public static native double createTensorDim(Builder builder, Long size, double nameOffset);

  public static native double endTensorDim(Builder builder);

  public static native TensorDim getRootAsTensorDim(ByteBuffer bb, TensorDim obj);

  public static native TensorDim getRootAsTensorDim(ByteBuffer bb);

  public static native TensorDim getSizePrefixedRootAsTensorDim(ByteBuffer bb, TensorDim obj);

  public static native TensorDim getSizePrefixedRootAsTensorDim(ByteBuffer bb);

  public static native void startTensorDim(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native TensorDim __init(double i, ByteBuffer bb);

  public native TensorDim.NameUnionType name();

  public native TensorDim.NameUnionType name(int optionalEncoding);

  public native Long size();
}

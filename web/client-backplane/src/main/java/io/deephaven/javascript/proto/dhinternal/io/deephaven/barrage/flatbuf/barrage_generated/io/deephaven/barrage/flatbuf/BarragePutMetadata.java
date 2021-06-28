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
        "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarragePutMetadata",
    namespace = JsPackage.GLOBAL)
public class BarragePutMetadata {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface CreateRpcTicketVectorDataUnionType {
    @JsOverlay
    static BarragePutMetadata.CreateRpcTicketVectorDataUnionType of(Object o) {
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

  public static native void addRpcTicket(Builder builder, double rpcTicketOffset);

  public static native void addSequence(Builder builder, Long sequence);

  public static native double createBarragePutMetadata(
      Builder builder, double rpcTicketOffset, Long sequence);

  @Deprecated
  public static native double createRpcTicketVector(
      Builder builder, BarragePutMetadata.CreateRpcTicketVectorDataUnionType data);

  @JsOverlay
  @Deprecated
  public static final double createRpcTicketVector(Builder builder, Int8Array data) {
    return createRpcTicketVector(
        builder, Js.<BarragePutMetadata.CreateRpcTicketVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createRpcTicketVector(Builder builder, JsArray<Double> data) {
    return createRpcTicketVector(
        builder, Js.<BarragePutMetadata.CreateRpcTicketVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createRpcTicketVector(Builder builder, Uint8Array data) {
    return createRpcTicketVector(
        builder, Js.<BarragePutMetadata.CreateRpcTicketVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createRpcTicketVector(Builder builder, double[] data) {
    return createRpcTicketVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double endBarragePutMetadata(Builder builder);

  public static native BarragePutMetadata getRootAsBarragePutMetadata(
      ByteBuffer bb, BarragePutMetadata obj);

  public static native BarragePutMetadata getRootAsBarragePutMetadata(ByteBuffer bb);

  public static native BarragePutMetadata getSizePrefixedRootAsBarragePutMetadata(
      ByteBuffer bb, BarragePutMetadata obj);

  public static native BarragePutMetadata getSizePrefixedRootAsBarragePutMetadata(ByteBuffer bb);

  public static native void startBarragePutMetadata(Builder builder);

  public static native void startRpcTicketVector(Builder builder, double numElems);

  public ByteBuffer bb;
  public double bb_pos;

  public native BarragePutMetadata __init(double i, ByteBuffer bb);

  public native double rpcTicket(double index);

  public native Int8Array rpcTicketArray();

  public native double rpcTicketLength();

  public native Long sequence();
}

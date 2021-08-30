package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.Int8Array;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.RefreshSessionRequest",
    namespace = JsPackage.GLOBAL)
public class RefreshSessionRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface CreateSessionVectorDataUnionType {
    @JsOverlay
    static RefreshSessionRequest.CreateSessionVectorDataUnionType of(Object o) {
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

  public static native void addSession(Builder builder, double sessionOffset);

  public static native double createRefreshSessionRequest(Builder builder, double sessionOffset);

  @Deprecated
  public static native double createSessionVector(
      Builder builder, RefreshSessionRequest.CreateSessionVectorDataUnionType data);

  @JsOverlay
  @Deprecated
  public static final double createSessionVector(Builder builder, Int8Array data) {
    return createSessionVector(
        builder, Js.<RefreshSessionRequest.CreateSessionVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createSessionVector(Builder builder, JsArray<Double> data) {
    return createSessionVector(
        builder, Js.<RefreshSessionRequest.CreateSessionVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createSessionVector(Builder builder, Uint8Array data) {
    return createSessionVector(
        builder, Js.<RefreshSessionRequest.CreateSessionVectorDataUnionType>uncheckedCast(data));
  }

  @JsOverlay
  @Deprecated
  public static final double createSessionVector(Builder builder, double[] data) {
    return createSessionVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double endRefreshSessionRequest(Builder builder);

  public static native RefreshSessionRequest getRootAsRefreshSessionRequest(
      ByteBuffer bb, RefreshSessionRequest obj);

  public static native RefreshSessionRequest getRootAsRefreshSessionRequest(ByteBuffer bb);

  public static native RefreshSessionRequest getSizePrefixedRootAsRefreshSessionRequest(
      ByteBuffer bb, RefreshSessionRequest obj);

  public static native RefreshSessionRequest getSizePrefixedRootAsRefreshSessionRequest(
      ByteBuffer bb);

  public static native void startRefreshSessionRequest(Builder builder);

  public static native void startSessionVector(Builder builder, double numElems);

  public ByteBuffer bb;
  public double bb_pos;

  public native RefreshSessionRequest __init(double i, ByteBuffer bb);

  public native double session(double index);

  public native Int8Array sessionArray();

  public native double sessionLength();
}

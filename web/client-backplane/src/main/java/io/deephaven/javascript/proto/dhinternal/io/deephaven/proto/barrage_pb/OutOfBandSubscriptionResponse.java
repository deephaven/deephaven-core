package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.barrage_pb.OutOfBandSubscriptionResponse",
    namespace = JsPackage.GLOBAL)
public class OutOfBandSubscriptionResponse {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static OutOfBandSubscriptionResponse.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    boolean isSubscriptionfound();

    @JsProperty
    void setSubscriptionfound(boolean subscriptionfound);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static OutOfBandSubscriptionResponse.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    boolean isSubscriptionfound();

    @JsProperty
    void setSubscriptionfound(boolean subscriptionfound);
  }

  public static native OutOfBandSubscriptionResponse deserializeBinary(Uint8Array bytes);

  public static native OutOfBandSubscriptionResponse deserializeBinaryFromReader(
      OutOfBandSubscriptionResponse message, Object reader);

  public static native void serializeBinaryToWriter(
      OutOfBandSubscriptionResponse message, Object writer);

  public static native OutOfBandSubscriptionResponse.ToObjectReturnType toObject(
      boolean includeInstance, OutOfBandSubscriptionResponse msg);

  public native boolean getSubscriptionfound();

  public native Uint8Array serializeBinary();

  public native void setSubscriptionfound(boolean value);

  public native OutOfBandSubscriptionResponse.ToObjectReturnType0 toObject();

  public native OutOfBandSubscriptionResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}

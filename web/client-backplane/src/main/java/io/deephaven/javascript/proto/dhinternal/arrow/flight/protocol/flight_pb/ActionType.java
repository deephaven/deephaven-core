package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb.ActionType",
    namespace = JsPackage.GLOBAL)
public class ActionType {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static ActionType.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getDescription();

    @JsProperty
    String getType();

    @JsProperty
    void setDescription(String description);

    @JsProperty
    void setType(String type);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static ActionType.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getDescription();

    @JsProperty
    String getType();

    @JsProperty
    void setDescription(String description);

    @JsProperty
    void setType(String type);
  }

  public static native ActionType deserializeBinary(Uint8Array bytes);

  public static native ActionType deserializeBinaryFromReader(ActionType message, Object reader);

  public static native void serializeBinaryToWriter(ActionType message, Object writer);

  public static native ActionType.ToObjectReturnType toObject(
      boolean includeInstance, ActionType msg);

  public native String getDescription();

  public native String getType();

  public native Uint8Array serializeBinary();

  public native void setDescription(String value);

  public native void setType(String value);

  public native ActionType.ToObjectReturnType0 toObject();

  public native ActionType.ToObjectReturnType0 toObject(boolean includeInstance);
}

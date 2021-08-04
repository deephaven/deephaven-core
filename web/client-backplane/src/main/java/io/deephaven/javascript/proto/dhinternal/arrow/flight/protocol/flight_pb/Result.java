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
    name = "dhinternal.arrow.flight.protocol.Flight_pb.Result",
    namespace = JsPackage.GLOBAL)
public class Result {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface GetBodyUnionType {
    @JsOverlay
    static Result.GetBodyUnionType of(Object o) {
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

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface SetBodyValueUnionType {
    @JsOverlay
    static Result.SetBodyValueUnionType of(Object o) {
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

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetBodyUnionType {
      @JsOverlay
      static Result.ToObjectReturnType.GetBodyUnionType of(Object o) {
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

    @JsOverlay
    static Result.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Result.ToObjectReturnType.GetBodyUnionType getBody();

    @JsProperty
    void setBody(Result.ToObjectReturnType.GetBodyUnionType body);

    @JsOverlay
    default void setBody(String body) {
      setBody(Js.<Result.ToObjectReturnType.GetBodyUnionType>uncheckedCast(body));
    }

    @JsOverlay
    default void setBody(Uint8Array body) {
      setBody(Js.<Result.ToObjectReturnType.GetBodyUnionType>uncheckedCast(body));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetBodyUnionType {
      @JsOverlay
      static Result.ToObjectReturnType0.GetBodyUnionType of(Object o) {
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

    @JsOverlay
    static Result.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Result.ToObjectReturnType0.GetBodyUnionType getBody();

    @JsProperty
    void setBody(Result.ToObjectReturnType0.GetBodyUnionType body);

    @JsOverlay
    default void setBody(String body) {
      setBody(Js.<Result.ToObjectReturnType0.GetBodyUnionType>uncheckedCast(body));
    }

    @JsOverlay
    default void setBody(Uint8Array body) {
      setBody(Js.<Result.ToObjectReturnType0.GetBodyUnionType>uncheckedCast(body));
    }
  }

  public static native Result deserializeBinary(Uint8Array bytes);

  public static native Result deserializeBinaryFromReader(Result message, Object reader);

  public static native void serializeBinaryToWriter(Result message, Object writer);

  public static native Result.ToObjectReturnType toObject(boolean includeInstance, Result msg);

  public native Result.GetBodyUnionType getBody();

  public native String getBody_asB64();

  public native Uint8Array getBody_asU8();

  public native Uint8Array serializeBinary();

  public native void setBody(Result.SetBodyValueUnionType value);

  @JsOverlay
  public final void setBody(String value) {
    setBody(Js.<Result.SetBodyValueUnionType>uncheckedCast(value));
  }

  @JsOverlay
  public final void setBody(Uint8Array value) {
    setBody(Js.<Result.SetBodyValueUnionType>uncheckedCast(value));
  }

  public native Result.ToObjectReturnType0 toObject();

  public native Result.ToObjectReturnType0 toObject(boolean includeInstance);
}

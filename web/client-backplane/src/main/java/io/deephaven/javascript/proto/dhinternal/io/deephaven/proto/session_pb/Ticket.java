package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.session_pb.Ticket",
    namespace = JsPackage.GLOBAL)
public class Ticket {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface GetIdUnionType {
    @JsOverlay
    static Ticket.GetIdUnionType of(Object o) {
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
  public interface SetIdValueUnionType {
    @JsOverlay
    static Ticket.SetIdValueUnionType of(Object o) {
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
    public interface GetIdUnionType {
      @JsOverlay
      static Ticket.ToObjectReturnType.GetIdUnionType of(Object o) {
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
    static Ticket.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Ticket.ToObjectReturnType.GetIdUnionType getId();

    @JsProperty
    void setId(Ticket.ToObjectReturnType.GetIdUnionType id);

    @JsOverlay
    default void setId(String id) {
      setId(Js.<Ticket.ToObjectReturnType.GetIdUnionType>uncheckedCast(id));
    }

    @JsOverlay
    default void setId(Uint8Array id) {
      setId(Js.<Ticket.ToObjectReturnType.GetIdUnionType>uncheckedCast(id));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetIdUnionType {
      @JsOverlay
      static Ticket.ToObjectReturnType0.GetIdUnionType of(Object o) {
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
    static Ticket.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Ticket.ToObjectReturnType0.GetIdUnionType getId();

    @JsProperty
    void setId(Ticket.ToObjectReturnType0.GetIdUnionType id);

    @JsOverlay
    default void setId(String id) {
      setId(Js.<Ticket.ToObjectReturnType0.GetIdUnionType>uncheckedCast(id));
    }

    @JsOverlay
    default void setId(Uint8Array id) {
      setId(Js.<Ticket.ToObjectReturnType0.GetIdUnionType>uncheckedCast(id));
    }
  }

  public static native Ticket deserializeBinary(Uint8Array bytes);

  public static native Ticket deserializeBinaryFromReader(Ticket message, Object reader);

  public static native void serializeBinaryToWriter(Ticket message, Object writer);

  public static native Ticket.ToObjectReturnType toObject(boolean includeInstance, Ticket msg);

  public native Ticket.GetIdUnionType getId();

  public native String getId_asB64();

  public native Uint8Array getId_asU8();

  public native Uint8Array serializeBinary();

  public native void setId(Ticket.SetIdValueUnionType value);

  @JsOverlay
  public final void setId(String value) {
    setId(Js.<Ticket.SetIdValueUnionType>uncheckedCast(value));
  }

  @JsOverlay
  public final void setId(Uint8Array value) {
    setId(Js.<Ticket.SetIdValueUnionType>uncheckedCast(value));
  }

  public native Ticket.ToObjectReturnType0 toObject();

  public native Ticket.ToObjectReturnType0 toObject(boolean includeInstance);
}

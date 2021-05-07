package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.CancelCommandRequest",
    namespace = JsPackage.GLOBAL)
public class CancelCommandRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(
            Object o) {
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
      static CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static CancelCommandRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Object getCommandid();

    @JsProperty
    CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    void setCommandid(Object commandid);

    @JsProperty
    void setConsoleid(CancelCommandRequest.ToObjectReturnType.ConsoleidFieldType consoleid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
            Object o) {
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
      static CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static CancelCommandRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Object getCommandid();

    @JsProperty
    CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    void setCommandid(Object commandid);

    @JsProperty
    void setConsoleid(CancelCommandRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);
  }

  public static native CancelCommandRequest deserializeBinary(Uint8Array bytes);

  public static native CancelCommandRequest deserializeBinaryFromReader(
      CancelCommandRequest message, Object reader);

  public static native void serializeBinaryToWriter(CancelCommandRequest message, Object writer);

  public static native CancelCommandRequest.ToObjectReturnType toObject(
      boolean includeInstance, CancelCommandRequest msg);

  public native void clearCommandid();

  public native void clearConsoleid();

  public native Ticket getCommandid();

  public native Ticket getConsoleid();

  public native boolean hasCommandid();

  public native boolean hasConsoleid();

  public native Uint8Array serializeBinary();

  public native void setCommandid();

  public native void setCommandid(Ticket value);

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native CancelCommandRequest.ToObjectReturnType0 toObject();

  public native CancelCommandRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

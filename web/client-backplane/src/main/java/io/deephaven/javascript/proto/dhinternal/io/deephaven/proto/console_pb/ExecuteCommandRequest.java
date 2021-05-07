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
    name = "dhinternal.io.deephaven.proto.console_pb.ExecuteCommandRequest",
    namespace = JsPackage.GLOBAL)
public class ExecuteCommandRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(
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
      static ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static ExecuteCommandRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getCode();

    @JsProperty
    ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    void setCode(String code);

    @JsProperty
    void setConsoleid(ExecuteCommandRequest.ToObjectReturnType.ConsoleidFieldType consoleid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
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
      static ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static ExecuteCommandRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getCode();

    @JsProperty
    ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    void setCode(String code);

    @JsProperty
    void setConsoleid(ExecuteCommandRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);
  }

  public static native ExecuteCommandRequest deserializeBinary(Uint8Array bytes);

  public static native ExecuteCommandRequest deserializeBinaryFromReader(
      ExecuteCommandRequest message, Object reader);

  public static native void serializeBinaryToWriter(ExecuteCommandRequest message, Object writer);

  public static native ExecuteCommandRequest.ToObjectReturnType toObject(
      boolean includeInstance, ExecuteCommandRequest msg);

  public native void clearConsoleid();

  public native String getCode();

  public native Ticket getConsoleid();

  public native boolean hasConsoleid();

  public native Uint8Array serializeBinary();

  public native void setCode(String value);

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native ExecuteCommandRequest.ToObjectReturnType0 toObject();

  public native ExecuteCommandRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

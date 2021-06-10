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
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType of(
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
      static ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
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
    ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

    @JsProperty
    void setCode(String code);

    @JsProperty
    void setConsoleId(ExecuteCommandRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType of(
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
      static ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
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
    ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

    @JsProperty
    void setCode(String code);

    @JsProperty
    void setConsoleId(ExecuteCommandRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);
  }

  public static native ExecuteCommandRequest deserializeBinary(Uint8Array bytes);

  public static native ExecuteCommandRequest deserializeBinaryFromReader(
      ExecuteCommandRequest message, Object reader);

  public static native void serializeBinaryToWriter(ExecuteCommandRequest message, Object writer);

  public static native ExecuteCommandRequest.ToObjectReturnType toObject(
      boolean includeInstance, ExecuteCommandRequest msg);

  public native void clearConsoleId();

  public native String getCode();

  public native Ticket getConsoleId();

  public native boolean hasConsoleId();

  public native Uint8Array serializeBinary();

  public native void setCode(String value);

  public native void setConsoleId();

  public native void setConsoleId(Ticket value);

  public native ExecuteCommandRequest.ToObjectReturnType0 toObject();

  public native ExecuteCommandRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

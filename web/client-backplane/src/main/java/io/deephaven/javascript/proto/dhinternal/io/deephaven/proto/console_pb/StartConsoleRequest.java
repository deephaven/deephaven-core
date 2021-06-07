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
    name = "dhinternal.io.deephaven.proto.console_pb.StartConsoleRequest",
    namespace = JsPackage.GLOBAL)
public class StartConsoleRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static StartConsoleRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType of(
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
      static StartConsoleRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      StartConsoleRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(StartConsoleRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<StartConsoleRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<StartConsoleRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static StartConsoleRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    StartConsoleRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    String getSessionType();

    @JsProperty
    void setResultId(StartConsoleRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setSessionType(String sessionType);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType of(
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
      static StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static StartConsoleRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    String getSessionType();

    @JsProperty
    void setResultId(StartConsoleRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setSessionType(String sessionType);
  }

  public static native StartConsoleRequest deserializeBinary(Uint8Array bytes);

  public static native StartConsoleRequest deserializeBinaryFromReader(
      StartConsoleRequest message, Object reader);

  public static native void serializeBinaryToWriter(StartConsoleRequest message, Object writer);

  public static native StartConsoleRequest.ToObjectReturnType toObject(
      boolean includeInstance, StartConsoleRequest msg);

  public native void clearResultId();

  public native Ticket getResultId();

  public native String getSessionType();

  public native boolean hasResultId();

  public native Uint8Array serializeBinary();

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setSessionType(String value);

  public native StartConsoleRequest.ToObjectReturnType0 toObject();

  public native StartConsoleRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

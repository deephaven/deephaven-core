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
    name = "dhinternal.io.deephaven.proto.console_pb.BindTableToVariableRequest",
    namespace = JsPackage.GLOBAL)
public class BindTableToVariableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType of(
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
      static BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static BindTableToVariableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

    @JsProperty
    Object getTableId();

    @JsProperty
    String getVariableName();

    @JsProperty
    void setConsoleId(BindTableToVariableRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

    @JsProperty
    void setTableId(Object tableId);

    @JsProperty
    void setVariableName(String variableName);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType of(
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
      static BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static BindTableToVariableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

    @JsProperty
    Object getTableId();

    @JsProperty
    String getVariableName();

    @JsProperty
    void setConsoleId(BindTableToVariableRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

    @JsProperty
    void setTableId(Object tableId);

    @JsProperty
    void setVariableName(String variableName);
  }

  public static native BindTableToVariableRequest deserializeBinary(Uint8Array bytes);

  public static native BindTableToVariableRequest deserializeBinaryFromReader(
      BindTableToVariableRequest message, Object reader);

  public static native void serializeBinaryToWriter(
      BindTableToVariableRequest message, Object writer);

  public static native BindTableToVariableRequest.ToObjectReturnType toObject(
      boolean includeInstance, BindTableToVariableRequest msg);

  public native void clearConsoleId();

  public native void clearTableId();

  public native Ticket getConsoleId();

  public native Ticket getTableId();

  public native String getVariableName();

  public native boolean hasConsoleId();

  public native boolean hasTableId();

  public native Uint8Array serializeBinary();

  public native void setConsoleId();

  public native void setConsoleId(Ticket value);

  public native void setTableId();

  public native void setTableId(Ticket value);

  public native void setVariableName(String value);

  public native BindTableToVariableRequest.ToObjectReturnType0 toObject();

  public native BindTableToVariableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

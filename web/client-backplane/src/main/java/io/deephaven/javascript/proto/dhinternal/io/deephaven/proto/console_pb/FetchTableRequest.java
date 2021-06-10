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
    name = "dhinternal.io.deephaven.proto.console_pb.FetchTableRequest",
    namespace = JsPackage.GLOBAL)
public class FetchTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType of(Object o) {
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
      static FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FetchTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

    @JsProperty
    Object getTableId();

    @JsProperty
    String getTableName();

    @JsProperty
    void setConsoleId(FetchTableRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

    @JsProperty
    void setTableId(Object tableId);

    @JsProperty
    void setTableName(String tableName);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType of(
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
      static FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FetchTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

    @JsProperty
    Object getTableId();

    @JsProperty
    String getTableName();

    @JsProperty
    void setConsoleId(FetchTableRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

    @JsProperty
    void setTableId(Object tableId);

    @JsProperty
    void setTableName(String tableName);
  }

  public static native FetchTableRequest deserializeBinary(Uint8Array bytes);

  public static native FetchTableRequest deserializeBinaryFromReader(
      FetchTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(FetchTableRequest message, Object writer);

  public static native FetchTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, FetchTableRequest msg);

  public native void clearConsoleId();

  public native void clearTableId();

  public native Ticket getConsoleId();

  public native Ticket getTableId();

  public native String getTableName();

  public native boolean hasConsoleId();

  public native boolean hasTableId();

  public native Uint8Array serializeBinary();

  public native void setConsoleId();

  public native void setConsoleId(Ticket value);

  public native void setTableId();

  public native void setTableId(Ticket value);

  public native void setTableName(String value);

  public native FetchTableRequest.ToObjectReturnType0 toObject();

  public native FetchTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

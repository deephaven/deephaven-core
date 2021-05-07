package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.JsArray;
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
    name = "dhinternal.io.deephaven.proto.console_pb.StartConsoleResponse",
    namespace = JsPackage.GLOBAL)
public class StartConsoleResponse {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static StartConsoleResponse.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(
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
      static StartConsoleResponse.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      StartConsoleResponse.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(StartConsoleResponse.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<StartConsoleResponse.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<StartConsoleResponse.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static StartConsoleResponse.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    StartConsoleResponse.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    JsArray<String> getTablenamesList();

    @JsProperty
    JsArray<String> getWidgetnamesList();

    @JsProperty
    void setResultid(StartConsoleResponse.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setTablenamesList(JsArray<String> tablenamesList);

    @JsOverlay
    default void setTablenamesList(String[] tablenamesList) {
      setTablenamesList(Js.<JsArray<String>>uncheckedCast(tablenamesList));
    }

    @JsProperty
    void setWidgetnamesList(JsArray<String> widgetnamesList);

    @JsOverlay
    default void setWidgetnamesList(String[] widgetnamesList) {
      setWidgetnamesList(Js.<JsArray<String>>uncheckedCast(widgetnamesList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static StartConsoleResponse.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static StartConsoleResponse.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      StartConsoleResponse.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(StartConsoleResponse.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<StartConsoleResponse.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<StartConsoleResponse.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static StartConsoleResponse.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    StartConsoleResponse.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    JsArray<String> getTablenamesList();

    @JsProperty
    JsArray<String> getWidgetnamesList();

    @JsProperty
    void setResultid(StartConsoleResponse.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setTablenamesList(JsArray<String> tablenamesList);

    @JsOverlay
    default void setTablenamesList(String[] tablenamesList) {
      setTablenamesList(Js.<JsArray<String>>uncheckedCast(tablenamesList));
    }

    @JsProperty
    void setWidgetnamesList(JsArray<String> widgetnamesList);

    @JsOverlay
    default void setWidgetnamesList(String[] widgetnamesList) {
      setWidgetnamesList(Js.<JsArray<String>>uncheckedCast(widgetnamesList));
    }
  }

  public static native StartConsoleResponse deserializeBinary(Uint8Array bytes);

  public static native StartConsoleResponse deserializeBinaryFromReader(
      StartConsoleResponse message, Object reader);

  public static native void serializeBinaryToWriter(StartConsoleResponse message, Object writer);

  public static native StartConsoleResponse.ToObjectReturnType toObject(
      boolean includeInstance, StartConsoleResponse msg);

  public native String addTablenames(String value, double index);

  public native String addTablenames(String value);

  public native String addWidgetnames(String value, double index);

  public native String addWidgetnames(String value);

  public native void clearResultid();

  public native void clearTablenamesList();

  public native void clearWidgetnamesList();

  public native Ticket getResultid();

  public native JsArray<String> getTablenamesList();

  public native JsArray<String> getWidgetnamesList();

  public native boolean hasResultid();

  public native Uint8Array serializeBinary();

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setTablenamesList(JsArray<String> value);

  @JsOverlay
  public final void setTablenamesList(String[] value) {
    setTablenamesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setWidgetnamesList(JsArray<String> value);

  @JsOverlay
  public final void setWidgetnamesList(String[] value) {
    setWidgetnamesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native StartConsoleResponse.ToObjectReturnType0 toObject();

  public native StartConsoleResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}

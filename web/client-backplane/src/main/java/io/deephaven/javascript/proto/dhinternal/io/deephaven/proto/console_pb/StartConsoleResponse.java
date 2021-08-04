package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
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
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static StartConsoleResponse.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
      static StartConsoleResponse.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      StartConsoleResponse.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          StartConsoleResponse.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<StartConsoleResponse.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<StartConsoleResponse.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsOverlay
    static StartConsoleResponse.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    StartConsoleResponse.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    JsArray<String> getTableNamesList();

    @JsProperty
    JsArray<String> getWidgetNamesList();

    @JsProperty
    void setResultId(StartConsoleResponse.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setTableNamesList(JsArray<String> tableNamesList);

    @JsOverlay
    default void setTableNamesList(String[] tableNamesList) {
      setTableNamesList(Js.<JsArray<String>>uncheckedCast(tableNamesList));
    }

    @JsProperty
    void setWidgetNamesList(JsArray<String> widgetNamesList);

    @JsOverlay
    default void setWidgetNamesList(String[] widgetNamesList) {
      setWidgetNamesList(Js.<JsArray<String>>uncheckedCast(widgetNamesList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
      static StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsOverlay
    static StartConsoleResponse.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    JsArray<String> getTableNamesList();

    @JsProperty
    JsArray<String> getWidgetNamesList();

    @JsProperty
    void setResultId(StartConsoleResponse.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setTableNamesList(JsArray<String> tableNamesList);

    @JsOverlay
    default void setTableNamesList(String[] tableNamesList) {
      setTableNamesList(Js.<JsArray<String>>uncheckedCast(tableNamesList));
    }

    @JsProperty
    void setWidgetNamesList(JsArray<String> widgetNamesList);

    @JsOverlay
    default void setWidgetNamesList(String[] widgetNamesList) {
      setWidgetNamesList(Js.<JsArray<String>>uncheckedCast(widgetNamesList));
    }
  }

  public static native StartConsoleResponse deserializeBinary(Uint8Array bytes);

  public static native StartConsoleResponse deserializeBinaryFromReader(
      StartConsoleResponse message, Object reader);

  public static native void serializeBinaryToWriter(StartConsoleResponse message, Object writer);

  public static native StartConsoleResponse.ToObjectReturnType toObject(
      boolean includeInstance, StartConsoleResponse msg);

  public native String addTableNames(String value, double index);

  public native String addTableNames(String value);

  public native String addWidgetNames(String value, double index);

  public native String addWidgetNames(String value);

  public native void clearResultId();

  public native void clearTableNamesList();

  public native void clearWidgetNamesList();

  public native Ticket getResultId();

  public native JsArray<String> getTableNamesList();

  public native JsArray<String> getWidgetNamesList();

  public native boolean hasResultId();

  public native Uint8Array serializeBinary();

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setTableNamesList(JsArray<String> value);

  @JsOverlay
  public final void setTableNamesList(String[] value) {
    setTableNamesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setWidgetNamesList(JsArray<String> value);

  @JsOverlay
  public final void setWidgetNamesList(String[] value) {
    setWidgetNamesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native StartConsoleResponse.ToObjectReturnType0 toObject();

  public native StartConsoleResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}

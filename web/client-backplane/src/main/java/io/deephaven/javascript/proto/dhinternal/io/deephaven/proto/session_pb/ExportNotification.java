package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.exportnotification.StateMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.session_pb.ExportNotification",
    namespace = JsPackage.GLOBAL)
public class ExportNotification {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TicketFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExportNotification.ToObjectReturnType.TicketFieldType.GetIdUnionType of(Object o) {
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
      static ExportNotification.ToObjectReturnType.TicketFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportNotification.ToObjectReturnType.TicketFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExportNotification.ToObjectReturnType.TicketFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<ExportNotification.ToObjectReturnType.TicketFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<ExportNotification.ToObjectReturnType.TicketFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static ExportNotification.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getContext();

    @JsProperty
    String getDependenthandle();

    @JsProperty
    double getExportstate();

    @JsProperty
    ExportNotification.ToObjectReturnType.TicketFieldType getTicket();

    @JsProperty
    void setContext(String context);

    @JsProperty
    void setDependenthandle(String dependenthandle);

    @JsProperty
    void setExportstate(double exportstate);

    @JsProperty
    void setTicket(ExportNotification.ToObjectReturnType.TicketFieldType ticket);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TicketFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExportNotification.ToObjectReturnType0.TicketFieldType.GetIdUnionType of(Object o) {
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
      static ExportNotification.ToObjectReturnType0.TicketFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportNotification.ToObjectReturnType0.TicketFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExportNotification.ToObjectReturnType0.TicketFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<ExportNotification.ToObjectReturnType0.TicketFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<ExportNotification.ToObjectReturnType0.TicketFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static ExportNotification.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getContext();

    @JsProperty
    String getDependenthandle();

    @JsProperty
    double getExportstate();

    @JsProperty
    ExportNotification.ToObjectReturnType0.TicketFieldType getTicket();

    @JsProperty
    void setContext(String context);

    @JsProperty
    void setDependenthandle(String dependenthandle);

    @JsProperty
    void setExportstate(double exportstate);

    @JsProperty
    void setTicket(ExportNotification.ToObjectReturnType0.TicketFieldType ticket);
  }

  public static StateMap State;

  public static native ExportNotification deserializeBinary(Uint8Array bytes);

  public static native ExportNotification deserializeBinaryFromReader(
      ExportNotification message, Object reader);

  public static native void serializeBinaryToWriter(ExportNotification message, Object writer);

  public static native ExportNotification.ToObjectReturnType toObject(
      boolean includeInstance, ExportNotification msg);

  public native void clearTicket();

  public native String getContext();

  public native String getDependenthandle();

  public native double getExportstate();

  public native Ticket getTicket();

  public native boolean hasTicket();

  public native Uint8Array serializeBinary();

  public native void setContext(String value);

  public native void setDependenthandle(String value);

  public native void setExportstate(double value);

  public native void setTicket();

  public native void setTicket(Ticket value);

  public native ExportNotification.ToObjectReturnType0 toObject();

  public native ExportNotification.ToObjectReturnType0 toObject(boolean includeInstance);
}

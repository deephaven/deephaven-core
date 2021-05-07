package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb;

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
    name = "dhinternal.io.deephaven.proto.barrage_pb.SubscriptionRequest",
    namespace = JsPackage.GLOBAL)
public class SubscriptionRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface GetColumnsUnionType {
    @JsOverlay
    static SubscriptionRequest.GetColumnsUnionType of(Object o) {
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
  public interface GetViewportUnionType {
    @JsOverlay
    static SubscriptionRequest.GetViewportUnionType of(Object o) {
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
  public interface SetColumnsValueUnionType {
    @JsOverlay
    static SubscriptionRequest.SetColumnsValueUnionType of(Object o) {
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
  public interface SetViewportValueUnionType {
    @JsOverlay
    static SubscriptionRequest.SetViewportValueUnionType of(Object o) {
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
    public interface GetColumnsUnionType {
      @JsOverlay
      static SubscriptionRequest.ToObjectReturnType.GetColumnsUnionType of(Object o) {
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
    public interface GetViewportUnionType {
      @JsOverlay
      static SubscriptionRequest.ToObjectReturnType.GetViewportUnionType of(Object o) {
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
    public interface TicketFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SubscriptionRequest.ToObjectReturnType.TicketFieldType.GetIdUnionType of(Object o) {
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
      static SubscriptionRequest.ToObjectReturnType.TicketFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SubscriptionRequest.ToObjectReturnType.TicketFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SubscriptionRequest.ToObjectReturnType.TicketFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<SubscriptionRequest.ToObjectReturnType.TicketFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<SubscriptionRequest.ToObjectReturnType.TicketFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static SubscriptionRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SubscriptionRequest.ToObjectReturnType.GetColumnsUnionType getColumns();

    @JsProperty
    Object getExportid();

    @JsProperty
    String getSequence();

    @JsProperty
    SubscriptionRequest.ToObjectReturnType.TicketFieldType getTicket();

    @JsProperty
    String getUpdateintervalms();

    @JsProperty
    SubscriptionRequest.ToObjectReturnType.GetViewportUnionType getViewport();

    @JsProperty
    boolean isUsedeephavennulls();

    @JsProperty
    void setColumns(SubscriptionRequest.ToObjectReturnType.GetColumnsUnionType columns);

    @JsOverlay
    default void setColumns(String columns) {
      setColumns(
          Js.<SubscriptionRequest.ToObjectReturnType.GetColumnsUnionType>uncheckedCast(columns));
    }

    @JsOverlay
    default void setColumns(Uint8Array columns) {
      setColumns(
          Js.<SubscriptionRequest.ToObjectReturnType.GetColumnsUnionType>uncheckedCast(columns));
    }

    @JsProperty
    void setExportid(Object exportid);

    @JsProperty
    void setSequence(String sequence);

    @JsProperty
    void setTicket(SubscriptionRequest.ToObjectReturnType.TicketFieldType ticket);

    @JsProperty
    void setUpdateintervalms(String updateintervalms);

    @JsProperty
    void setUsedeephavennulls(boolean usedeephavennulls);

    @JsProperty
    void setViewport(SubscriptionRequest.ToObjectReturnType.GetViewportUnionType viewport);

    @JsOverlay
    default void setViewport(String viewport) {
      setViewport(
          Js.<SubscriptionRequest.ToObjectReturnType.GetViewportUnionType>uncheckedCast(viewport));
    }

    @JsOverlay
    default void setViewport(Uint8Array viewport) {
      setViewport(
          Js.<SubscriptionRequest.ToObjectReturnType.GetViewportUnionType>uncheckedCast(viewport));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetColumnsUnionType {
      @JsOverlay
      static SubscriptionRequest.ToObjectReturnType0.GetColumnsUnionType of(Object o) {
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
    public interface GetViewportUnionType {
      @JsOverlay
      static SubscriptionRequest.ToObjectReturnType0.GetViewportUnionType of(Object o) {
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
    public interface TicketFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SubscriptionRequest.ToObjectReturnType0.TicketFieldType.GetIdUnionType of(Object o) {
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
      static SubscriptionRequest.ToObjectReturnType0.TicketFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SubscriptionRequest.ToObjectReturnType0.TicketFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SubscriptionRequest.ToObjectReturnType0.TicketFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<SubscriptionRequest.ToObjectReturnType0.TicketFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<SubscriptionRequest.ToObjectReturnType0.TicketFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static SubscriptionRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SubscriptionRequest.ToObjectReturnType0.GetColumnsUnionType getColumns();

    @JsProperty
    Object getExportid();

    @JsProperty
    String getSequence();

    @JsProperty
    SubscriptionRequest.ToObjectReturnType0.TicketFieldType getTicket();

    @JsProperty
    String getUpdateintervalms();

    @JsProperty
    SubscriptionRequest.ToObjectReturnType0.GetViewportUnionType getViewport();

    @JsProperty
    boolean isUsedeephavennulls();

    @JsProperty
    void setColumns(SubscriptionRequest.ToObjectReturnType0.GetColumnsUnionType columns);

    @JsOverlay
    default void setColumns(String columns) {
      setColumns(
          Js.<SubscriptionRequest.ToObjectReturnType0.GetColumnsUnionType>uncheckedCast(columns));
    }

    @JsOverlay
    default void setColumns(Uint8Array columns) {
      setColumns(
          Js.<SubscriptionRequest.ToObjectReturnType0.GetColumnsUnionType>uncheckedCast(columns));
    }

    @JsProperty
    void setExportid(Object exportid);

    @JsProperty
    void setSequence(String sequence);

    @JsProperty
    void setTicket(SubscriptionRequest.ToObjectReturnType0.TicketFieldType ticket);

    @JsProperty
    void setUpdateintervalms(String updateintervalms);

    @JsProperty
    void setUsedeephavennulls(boolean usedeephavennulls);

    @JsProperty
    void setViewport(SubscriptionRequest.ToObjectReturnType0.GetViewportUnionType viewport);

    @JsOverlay
    default void setViewport(String viewport) {
      setViewport(
          Js.<SubscriptionRequest.ToObjectReturnType0.GetViewportUnionType>uncheckedCast(viewport));
    }

    @JsOverlay
    default void setViewport(Uint8Array viewport) {
      setViewport(
          Js.<SubscriptionRequest.ToObjectReturnType0.GetViewportUnionType>uncheckedCast(viewport));
    }
  }

  public static native SubscriptionRequest deserializeBinary(Uint8Array bytes);

  public static native SubscriptionRequest deserializeBinaryFromReader(
      SubscriptionRequest message, Object reader);

  public static native void serializeBinaryToWriter(SubscriptionRequest message, Object writer);

  public static native SubscriptionRequest.ToObjectReturnType toObject(
      boolean includeInstance, SubscriptionRequest msg);

  public native void clearExportid();

  public native void clearTicket();

  public native SubscriptionRequest.GetColumnsUnionType getColumns();

  public native String getColumns_asB64();

  public native Uint8Array getColumns_asU8();

  public native Ticket getExportid();

  public native String getSequence();

  public native Ticket getTicket();

  public native String getUpdateintervalms();

  public native boolean getUsedeephavennulls();

  public native SubscriptionRequest.GetViewportUnionType getViewport();

  public native String getViewport_asB64();

  public native Uint8Array getViewport_asU8();

  public native boolean hasExportid();

  public native boolean hasTicket();

  public native Uint8Array serializeBinary();

  public native void setColumns(SubscriptionRequest.SetColumnsValueUnionType value);

  @JsOverlay
  public final void setColumns(String value) {
    setColumns(Js.<SubscriptionRequest.SetColumnsValueUnionType>uncheckedCast(value));
  }

  @JsOverlay
  public final void setColumns(Uint8Array value) {
    setColumns(Js.<SubscriptionRequest.SetColumnsValueUnionType>uncheckedCast(value));
  }

  public native void setExportid();

  public native void setExportid(Ticket value);

  public native void setSequence(String value);

  public native void setTicket();

  public native void setTicket(Ticket value);

  public native void setUpdateintervalms(String value);

  public native void setUsedeephavennulls(boolean value);

  public native void setViewport(SubscriptionRequest.SetViewportValueUnionType value);

  @JsOverlay
  public final void setViewport(String value) {
    setViewport(Js.<SubscriptionRequest.SetViewportValueUnionType>uncheckedCast(value));
  }

  @JsOverlay
  public final void setViewport(Uint8Array value) {
    setViewport(Js.<SubscriptionRequest.SetViewportValueUnionType>uncheckedCast(value));
  }

  public native SubscriptionRequest.ToObjectReturnType0 toObject();

  public native SubscriptionRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

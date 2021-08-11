package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

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
    name = "dhinternal.io.deephaven.proto.table_pb.SnapshotTableRequest",
    namespace = JsPackage.GLOBAL)
public class SnapshotTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LeftIdFieldType {
      @JsOverlay
      static SnapshotTableRequest.ToObjectReturnType.LeftIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchOffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchOffset(double batchOffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
      static SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsOverlay
    static SnapshotTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType.LeftIdFieldType getLeftId();

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    Object getRightId();

    @JsProperty
    JsArray<String> getStampColumnsList();

    @JsProperty
    boolean isDoInitialSnapshot();

    @JsProperty
    void setDoInitialSnapshot(boolean doInitialSnapshot);

    @JsProperty
    void setLeftId(SnapshotTableRequest.ToObjectReturnType.LeftIdFieldType leftId);

    @JsProperty
    void setResultId(SnapshotTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setRightId(Object rightId);

    @JsProperty
    void setStampColumnsList(JsArray<String> stampColumnsList);

    @JsOverlay
    default void setStampColumnsList(String[] stampColumnsList) {
      setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LeftIdFieldType {
      @JsOverlay
      static SnapshotTableRequest.ToObjectReturnType0.LeftIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchOffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchOffset(double batchOffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
      static SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsOverlay
    static SnapshotTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType0.LeftIdFieldType getLeftId();

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    Object getRightId();

    @JsProperty
    JsArray<String> getStampColumnsList();

    @JsProperty
    boolean isDoInitialSnapshot();

    @JsProperty
    void setDoInitialSnapshot(boolean doInitialSnapshot);

    @JsProperty
    void setLeftId(SnapshotTableRequest.ToObjectReturnType0.LeftIdFieldType leftId);

    @JsProperty
    void setResultId(SnapshotTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setRightId(Object rightId);

    @JsProperty
    void setStampColumnsList(JsArray<String> stampColumnsList);

    @JsOverlay
    default void setStampColumnsList(String[] stampColumnsList) {
      setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
    }
  }

  public static native SnapshotTableRequest deserializeBinary(Uint8Array bytes);

  public static native SnapshotTableRequest deserializeBinaryFromReader(
      SnapshotTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(SnapshotTableRequest message, Object writer);

  public static native SnapshotTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, SnapshotTableRequest msg);

  public native String addStampColumns(String value, double index);

  public native String addStampColumns(String value);

  public native void clearLeftId();

  public native void clearResultId();

  public native void clearRightId();

  public native void clearStampColumnsList();

  public native boolean getDoInitialSnapshot();

  public native TableReference getLeftId();

  public native Ticket getResultId();

  public native TableReference getRightId();

  public native JsArray<String> getStampColumnsList();

  public native boolean hasLeftId();

  public native boolean hasResultId();

  public native boolean hasRightId();

  public native Uint8Array serializeBinary();

  public native void setDoInitialSnapshot(boolean value);

  public native void setLeftId();

  public native void setLeftId(TableReference value);

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setRightId();

  public native void setRightId(TableReference value);

  public native void setStampColumnsList(JsArray<String> value);

  @JsOverlay
  public final void setStampColumnsList(String[] value) {
    setStampColumnsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native SnapshotTableRequest.ToObjectReturnType0 toObject();

  public native SnapshotTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

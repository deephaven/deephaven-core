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
    name = "dhinternal.io.deephaven.proto.table_pb.SortTableRequest",
    namespace = JsPackage.GLOBAL)
public class SortTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static SortTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
      static SortTableRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SortTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          SortTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<SortTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<SortTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SortsListFieldType {
      @JsOverlay
      static SortTableRequest.ToObjectReturnType.SortsListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnName();

      @JsProperty
      double getDirection();

      @JsProperty
      boolean isIsAbsolute();

      @JsProperty
      void setColumnName(String columnName);

      @JsProperty
      void setDirection(double direction);

      @JsProperty
      void setIsAbsolute(boolean isAbsolute);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static SortTableRequest.ToObjectReturnType.SourceIdFieldType create() {
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

    @JsOverlay
    static SortTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SortTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    JsArray<SortTableRequest.ToObjectReturnType.SortsListFieldType> getSortsList();

    @JsProperty
    SortTableRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

    @JsProperty
    void setResultId(SortTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setSortsList(JsArray<SortTableRequest.ToObjectReturnType.SortsListFieldType> sortsList);

    @JsOverlay
    default void setSortsList(SortTableRequest.ToObjectReturnType.SortsListFieldType[] sortsList) {
      setSortsList(
          Js.<JsArray<SortTableRequest.ToObjectReturnType.SortsListFieldType>>uncheckedCast(
              sortsList));
    }

    @JsProperty
    void setSourceId(SortTableRequest.ToObjectReturnType.SourceIdFieldType sourceId);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static SortTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
      static SortTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SortTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          SortTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<SortTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<SortTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SortsListFieldType {
      @JsOverlay
      static SortTableRequest.ToObjectReturnType0.SortsListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnName();

      @JsProperty
      double getDirection();

      @JsProperty
      boolean isIsAbsolute();

      @JsProperty
      void setColumnName(String columnName);

      @JsProperty
      void setDirection(double direction);

      @JsProperty
      void setIsAbsolute(boolean isAbsolute);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static SortTableRequest.ToObjectReturnType0.SourceIdFieldType create() {
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

    @JsOverlay
    static SortTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SortTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    JsArray<SortTableRequest.ToObjectReturnType0.SortsListFieldType> getSortsList();

    @JsProperty
    SortTableRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

    @JsProperty
    void setResultId(SortTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setSortsList(JsArray<SortTableRequest.ToObjectReturnType0.SortsListFieldType> sortsList);

    @JsOverlay
    default void setSortsList(SortTableRequest.ToObjectReturnType0.SortsListFieldType[] sortsList) {
      setSortsList(
          Js.<JsArray<SortTableRequest.ToObjectReturnType0.SortsListFieldType>>uncheckedCast(
              sortsList));
    }

    @JsProperty
    void setSourceId(SortTableRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
  }

  public static native SortTableRequest deserializeBinary(Uint8Array bytes);

  public static native SortTableRequest deserializeBinaryFromReader(
      SortTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(SortTableRequest message, Object writer);

  public static native SortTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, SortTableRequest msg);

  public native SortDescriptor addSorts();

  public native SortDescriptor addSorts(SortDescriptor value, double index);

  public native SortDescriptor addSorts(SortDescriptor value);

  public native void clearResultId();

  public native void clearSortsList();

  public native void clearSourceId();

  public native Ticket getResultId();

  public native JsArray<SortDescriptor> getSortsList();

  public native TableReference getSourceId();

  public native boolean hasResultId();

  public native boolean hasSourceId();

  public native Uint8Array serializeBinary();

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setSortsList(JsArray<SortDescriptor> value);

  @JsOverlay
  public final void setSortsList(SortDescriptor[] value) {
    setSortsList(Js.<JsArray<SortDescriptor>>uncheckedCast(value));
  }

  public native void setSourceId();

  public native void setSourceId(TableReference value);

  public native SortTableRequest.ToObjectReturnType0 toObject();

  public native SortTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

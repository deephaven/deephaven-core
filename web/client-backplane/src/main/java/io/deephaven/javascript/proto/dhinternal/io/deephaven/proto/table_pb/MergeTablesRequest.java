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
    name = "dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest",
    namespace = JsPackage.GLOBAL)
public class MergeTablesRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static MergeTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
      static MergeTablesRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      MergeTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          MergeTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<MergeTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<MergeTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdsListFieldType {
      @JsOverlay
      static MergeTablesRequest.ToObjectReturnType.SourceIdsListFieldType create() {
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
    static MergeTablesRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getKeyColumn();

    @JsProperty
    MergeTablesRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    JsArray<MergeTablesRequest.ToObjectReturnType.SourceIdsListFieldType> getSourceIdsList();

    @JsProperty
    void setKeyColumn(String keyColumn);

    @JsProperty
    void setResultId(MergeTablesRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setSourceIdsList(
        JsArray<MergeTablesRequest.ToObjectReturnType.SourceIdsListFieldType> sourceIdsList);

    @JsOverlay
    default void setSourceIdsList(
        MergeTablesRequest.ToObjectReturnType.SourceIdsListFieldType[] sourceIdsList) {
      setSourceIdsList(
          Js.<JsArray<MergeTablesRequest.ToObjectReturnType.SourceIdsListFieldType>>uncheckedCast(
              sourceIdsList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetTicketUnionType {
        @JsOverlay
        static MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
      static MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

      @JsProperty
      void setTicket(
          MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

      @JsOverlay
      default void setTicket(String ticket) {
        setTicket(
            Js
                .<MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }

      @JsOverlay
      default void setTicket(Uint8Array ticket) {
        setTicket(
            Js
                .<MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>
                    uncheckedCast(ticket));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdsListFieldType {
      @JsOverlay
      static MergeTablesRequest.ToObjectReturnType0.SourceIdsListFieldType create() {
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
    static MergeTablesRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getKeyColumn();

    @JsProperty
    MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    JsArray<MergeTablesRequest.ToObjectReturnType0.SourceIdsListFieldType> getSourceIdsList();

    @JsProperty
    void setKeyColumn(String keyColumn);

    @JsProperty
    void setResultId(MergeTablesRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setSourceIdsList(
        JsArray<MergeTablesRequest.ToObjectReturnType0.SourceIdsListFieldType> sourceIdsList);

    @JsOverlay
    default void setSourceIdsList(
        MergeTablesRequest.ToObjectReturnType0.SourceIdsListFieldType[] sourceIdsList) {
      setSourceIdsList(
          Js.<JsArray<MergeTablesRequest.ToObjectReturnType0.SourceIdsListFieldType>>uncheckedCast(
              sourceIdsList));
    }
  }

  public static native MergeTablesRequest deserializeBinary(Uint8Array bytes);

  public static native MergeTablesRequest deserializeBinaryFromReader(
      MergeTablesRequest message, Object reader);

  public static native void serializeBinaryToWriter(MergeTablesRequest message, Object writer);

  public static native MergeTablesRequest.ToObjectReturnType toObject(
      boolean includeInstance, MergeTablesRequest msg);

  public native TableReference addSourceIds();

  public native TableReference addSourceIds(TableReference value, double index);

  public native TableReference addSourceIds(TableReference value);

  public native void clearResultId();

  public native void clearSourceIdsList();

  public native String getKeyColumn();

  public native Ticket getResultId();

  public native JsArray<TableReference> getSourceIdsList();

  public native boolean hasResultId();

  public native Uint8Array serializeBinary();

  public native void setKeyColumn(String value);

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setSourceIdsList(JsArray<TableReference> value);

  @JsOverlay
  public final void setSourceIdsList(TableReference[] value) {
    setSourceIdsList(Js.<JsArray<TableReference>>uncheckedCast(value));
  }

  public native MergeTablesRequest.ToObjectReturnType0 toObject();

  public native MergeTablesRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

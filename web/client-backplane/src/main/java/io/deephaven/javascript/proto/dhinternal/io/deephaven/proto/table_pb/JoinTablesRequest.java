package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.jointablesrequest.TypeMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.JoinTablesRequest",
    namespace = JsPackage.GLOBAL)
public class JoinTablesRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LeftidFieldType {
      @JsOverlay
      static JoinTablesRequest.ToObjectReturnType.LeftidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static JoinTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static JoinTablesRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JoinTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(JoinTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<JoinTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<JoinTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static JoinTablesRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnstoaddList();

    @JsProperty
    JsArray<String> getColumnstomatchList();

    @JsProperty
    double getJointype();

    @JsProperty
    JoinTablesRequest.ToObjectReturnType.LeftidFieldType getLeftid();

    @JsProperty
    JoinTablesRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    Object getRightid();

    @JsProperty
    void setColumnstoaddList(JsArray<String> columnstoaddList);

    @JsOverlay
    default void setColumnstoaddList(String[] columnstoaddList) {
      setColumnstoaddList(Js.<JsArray<String>>uncheckedCast(columnstoaddList));
    }

    @JsProperty
    void setColumnstomatchList(JsArray<String> columnstomatchList);

    @JsOverlay
    default void setColumnstomatchList(String[] columnstomatchList) {
      setColumnstomatchList(Js.<JsArray<String>>uncheckedCast(columnstomatchList));
    }

    @JsProperty
    void setJointype(double jointype);

    @JsProperty
    void setLeftid(JoinTablesRequest.ToObjectReturnType.LeftidFieldType leftid);

    @JsProperty
    void setResultid(JoinTablesRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setRightid(Object rightid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LeftidFieldType {
      @JsOverlay
      static JoinTablesRequest.ToObjectReturnType0.LeftidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static JoinTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static JoinTablesRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JoinTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(JoinTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<JoinTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<JoinTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static JoinTablesRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnstoaddList();

    @JsProperty
    JsArray<String> getColumnstomatchList();

    @JsProperty
    double getJointype();

    @JsProperty
    JoinTablesRequest.ToObjectReturnType0.LeftidFieldType getLeftid();

    @JsProperty
    JoinTablesRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    Object getRightid();

    @JsProperty
    void setColumnstoaddList(JsArray<String> columnstoaddList);

    @JsOverlay
    default void setColumnstoaddList(String[] columnstoaddList) {
      setColumnstoaddList(Js.<JsArray<String>>uncheckedCast(columnstoaddList));
    }

    @JsProperty
    void setColumnstomatchList(JsArray<String> columnstomatchList);

    @JsOverlay
    default void setColumnstomatchList(String[] columnstomatchList) {
      setColumnstomatchList(Js.<JsArray<String>>uncheckedCast(columnstomatchList));
    }

    @JsProperty
    void setJointype(double jointype);

    @JsProperty
    void setLeftid(JoinTablesRequest.ToObjectReturnType0.LeftidFieldType leftid);

    @JsProperty
    void setResultid(JoinTablesRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setRightid(Object rightid);
  }

  public static TypeMap Type;

  public static native JoinTablesRequest deserializeBinary(Uint8Array bytes);

  public static native JoinTablesRequest deserializeBinaryFromReader(
      JoinTablesRequest message, Object reader);

  public static native void serializeBinaryToWriter(JoinTablesRequest message, Object writer);

  public static native JoinTablesRequest.ToObjectReturnType toObject(
      boolean includeInstance, JoinTablesRequest msg);

  public native String addColumnstoadd(String value, double index);

  public native String addColumnstoadd(String value);

  public native String addColumnstomatch(String value, double index);

  public native String addColumnstomatch(String value);

  public native void clearColumnstoaddList();

  public native void clearColumnstomatchList();

  public native void clearLeftid();

  public native void clearResultid();

  public native void clearRightid();

  public native JsArray<String> getColumnstoaddList();

  public native JsArray<String> getColumnstomatchList();

  public native double getJointype();

  public native TableReference getLeftid();

  public native Ticket getResultid();

  public native TableReference getRightid();

  public native boolean hasLeftid();

  public native boolean hasResultid();

  public native boolean hasRightid();

  public native Uint8Array serializeBinary();

  public native void setColumnstoaddList(JsArray<String> value);

  @JsOverlay
  public final void setColumnstoaddList(String[] value) {
    setColumnstoaddList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setColumnstomatchList(JsArray<String> value);

  @JsOverlay
  public final void setColumnstomatchList(String[] value) {
    setColumnstomatchList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setJointype(double value);

  public native void setLeftid();

  public native void setLeftid(TableReference value);

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setRightid();

  public native void setRightid(TableReference value);

  public native JoinTablesRequest.ToObjectReturnType0 toObject();

  public native JoinTablesRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

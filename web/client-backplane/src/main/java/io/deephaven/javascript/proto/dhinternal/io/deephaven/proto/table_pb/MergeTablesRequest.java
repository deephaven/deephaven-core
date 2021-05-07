package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

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
    name = "dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest",
    namespace = JsPackage.GLOBAL)
public class MergeTablesRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static MergeTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static MergeTablesRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      MergeTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(MergeTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<MergeTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<MergeTablesRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidsListFieldType {
      @JsOverlay
      static MergeTablesRequest.ToObjectReturnType.SourceidsListFieldType create() {
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

    @JsOverlay
    static MergeTablesRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getKeycolumn();

    @JsProperty
    MergeTablesRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    JsArray<MergeTablesRequest.ToObjectReturnType.SourceidsListFieldType> getSourceidsList();

    @JsProperty
    void setKeycolumn(String keycolumn);

    @JsProperty
    void setResultid(MergeTablesRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceidsList(
        JsArray<MergeTablesRequest.ToObjectReturnType.SourceidsListFieldType> sourceidsList);

    @JsOverlay
    default void setSourceidsList(
        MergeTablesRequest.ToObjectReturnType.SourceidsListFieldType[] sourceidsList) {
      setSourceidsList(
          Js.<JsArray<MergeTablesRequest.ToObjectReturnType.SourceidsListFieldType>>uncheckedCast(
              sourceidsList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static MergeTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static MergeTablesRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      MergeTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(MergeTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<MergeTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<MergeTablesRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidsListFieldType {
      @JsOverlay
      static MergeTablesRequest.ToObjectReturnType0.SourceidsListFieldType create() {
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

    @JsOverlay
    static MergeTablesRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getKeycolumn();

    @JsProperty
    MergeTablesRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    JsArray<MergeTablesRequest.ToObjectReturnType0.SourceidsListFieldType> getSourceidsList();

    @JsProperty
    void setKeycolumn(String keycolumn);

    @JsProperty
    void setResultid(MergeTablesRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceidsList(
        JsArray<MergeTablesRequest.ToObjectReturnType0.SourceidsListFieldType> sourceidsList);

    @JsOverlay
    default void setSourceidsList(
        MergeTablesRequest.ToObjectReturnType0.SourceidsListFieldType[] sourceidsList) {
      setSourceidsList(
          Js.<JsArray<MergeTablesRequest.ToObjectReturnType0.SourceidsListFieldType>>uncheckedCast(
              sourceidsList));
    }
  }

  public static native MergeTablesRequest deserializeBinary(Uint8Array bytes);

  public static native MergeTablesRequest deserializeBinaryFromReader(
      MergeTablesRequest message, Object reader);

  public static native void serializeBinaryToWriter(MergeTablesRequest message, Object writer);

  public static native MergeTablesRequest.ToObjectReturnType toObject(
      boolean includeInstance, MergeTablesRequest msg);

  public native TableReference addSourceids();

  public native TableReference addSourceids(TableReference value, double index);

  public native TableReference addSourceids(TableReference value);

  public native void clearResultid();

  public native void clearSourceidsList();

  public native String getKeycolumn();

  public native Ticket getResultid();

  public native JsArray<TableReference> getSourceidsList();

  public native boolean hasResultid();

  public native Uint8Array serializeBinary();

  public native void setKeycolumn(String value);

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceidsList(JsArray<TableReference> value);

  @JsOverlay
  public final void setSourceidsList(TableReference[] value) {
    setSourceidsList(Js.<JsArray<TableReference>>uncheckedCast(value));
  }

  public native MergeTablesRequest.ToObjectReturnType0 toObject();

  public native MergeTablesRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

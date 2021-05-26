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
    name = "dhinternal.io.deephaven.proto.table_pb.UnstructuredFilterTableRequest",
    namespace = JsPackage.GLOBAL)
public class UnstructuredFilterTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType
            of(Object o) {
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
      static UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static UnstructuredFilterTableRequest.ToObjectReturnType.SourceIdFieldType create() {
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
    static UnstructuredFilterTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getFiltersList();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

    @JsProperty
    void setFiltersList(JsArray<String> filtersList);

    @JsOverlay
    default void setFiltersList(String[] filtersList) {
      setFiltersList(Js.<JsArray<String>>uncheckedCast(filtersList));
    }

    @JsProperty
    void setResultId(UnstructuredFilterTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setSourceId(UnstructuredFilterTableRequest.ToObjectReturnType.SourceIdFieldType sourceId);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType
            of(Object o) {
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
      static UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static UnstructuredFilterTableRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
    static UnstructuredFilterTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getFiltersList();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

    @JsProperty
    void setFiltersList(JsArray<String> filtersList);

    @JsOverlay
    default void setFiltersList(String[] filtersList) {
      setFiltersList(Js.<JsArray<String>>uncheckedCast(filtersList));
    }

    @JsProperty
    void setResultId(UnstructuredFilterTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setSourceId(UnstructuredFilterTableRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
  }

  public static native UnstructuredFilterTableRequest deserializeBinary(Uint8Array bytes);

  public static native UnstructuredFilterTableRequest deserializeBinaryFromReader(
      UnstructuredFilterTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(
      UnstructuredFilterTableRequest message, Object writer);

  public static native UnstructuredFilterTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, UnstructuredFilterTableRequest msg);

  public native String addFilters(String value, double index);

  public native String addFilters(String value);

  public native void clearFiltersList();

  public native void clearResultId();

  public native void clearSourceId();

  public native JsArray<String> getFiltersList();

  public native Ticket getResultId();

  public native TableReference getSourceId();

  public native boolean hasResultId();

  public native boolean hasSourceId();

  public native Uint8Array serializeBinary();

  public native void setFiltersList(JsArray<String> value);

  @JsOverlay
  public final void setFiltersList(String[] value) {
    setFiltersList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setSourceId();

  public native void setSourceId(TableReference value);

  public native UnstructuredFilterTableRequest.ToObjectReturnType0 toObject();

  public native UnstructuredFilterTableRequest.ToObjectReturnType0 toObject(
      boolean includeInstance);
}

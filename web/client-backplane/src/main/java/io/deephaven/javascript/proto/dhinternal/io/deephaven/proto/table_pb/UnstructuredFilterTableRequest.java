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
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType
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
      static UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static UnstructuredFilterTableRequest.ToObjectReturnType.SourceidFieldType create() {
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
    static UnstructuredFilterTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getFilterList();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsProperty
    void setFilterList(JsArray<String> filterList);

    @JsOverlay
    default void setFilterList(String[] filterList) {
      setFilterList(Js.<JsArray<String>>uncheckedCast(filterList));
    }

    @JsProperty
    void setResultid(UnstructuredFilterTableRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(UnstructuredFilterTableRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType
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
      static UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType
                        .GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static UnstructuredFilterTableRequest.ToObjectReturnType0.SourceidFieldType create() {
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
    static UnstructuredFilterTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getFilterList();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    UnstructuredFilterTableRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsProperty
    void setFilterList(JsArray<String> filterList);

    @JsOverlay
    default void setFilterList(String[] filterList) {
      setFilterList(Js.<JsArray<String>>uncheckedCast(filterList));
    }

    @JsProperty
    void setResultid(UnstructuredFilterTableRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(UnstructuredFilterTableRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static native UnstructuredFilterTableRequest deserializeBinary(Uint8Array bytes);

  public static native UnstructuredFilterTableRequest deserializeBinaryFromReader(
      UnstructuredFilterTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(
      UnstructuredFilterTableRequest message, Object writer);

  public static native UnstructuredFilterTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, UnstructuredFilterTableRequest msg);

  public native String addFilter(String value, double index);

  public native String addFilter(String value);

  public native void clearFilterList();

  public native void clearResultid();

  public native void clearSourceid();

  public native JsArray<String> getFilterList();

  public native Ticket getResultid();

  public native TableReference getSourceid();

  public native boolean hasResultid();

  public native boolean hasSourceid();

  public native Uint8Array serializeBinary();

  public native void setFilterList(JsArray<String> value);

  @JsOverlay
  public final void setFilterList(String[] value) {
    setFilterList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceid();

  public native void setSourceid(TableReference value);

  public native UnstructuredFilterTableRequest.ToObjectReturnType0 toObject();

  public native UnstructuredFilterTableRequest.ToObjectReturnType0 toObject(
      boolean includeInstance);
}

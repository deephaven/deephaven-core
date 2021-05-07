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
    name = "dhinternal.io.deephaven.proto.table_pb.UngroupRequest",
    namespace = JsPackage.GLOBAL)
public class UngroupRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static UngroupRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static UngroupRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      UngroupRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(UngroupRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<UngroupRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<UngroupRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static UngroupRequest.ToObjectReturnType.SourceidFieldType create() {
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
    static UngroupRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnstoungroupList();

    @JsProperty
    UngroupRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    UngroupRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsProperty
    boolean isNullfill();

    @JsProperty
    void setColumnstoungroupList(JsArray<String> columnstoungroupList);

    @JsOverlay
    default void setColumnstoungroupList(String[] columnstoungroupList) {
      setColumnstoungroupList(Js.<JsArray<String>>uncheckedCast(columnstoungroupList));
    }

    @JsProperty
    void setNullfill(boolean nullfill);

    @JsProperty
    void setResultid(UngroupRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(UngroupRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static UngroupRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static UngroupRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      UngroupRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(UngroupRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<UngroupRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<UngroupRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static UngroupRequest.ToObjectReturnType0.SourceidFieldType create() {
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
    static UngroupRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnstoungroupList();

    @JsProperty
    UngroupRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    UngroupRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsProperty
    boolean isNullfill();

    @JsProperty
    void setColumnstoungroupList(JsArray<String> columnstoungroupList);

    @JsOverlay
    default void setColumnstoungroupList(String[] columnstoungroupList) {
      setColumnstoungroupList(Js.<JsArray<String>>uncheckedCast(columnstoungroupList));
    }

    @JsProperty
    void setNullfill(boolean nullfill);

    @JsProperty
    void setResultid(UngroupRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(UngroupRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static native UngroupRequest deserializeBinary(Uint8Array bytes);

  public static native UngroupRequest deserializeBinaryFromReader(
      UngroupRequest message, Object reader);

  public static native void serializeBinaryToWriter(UngroupRequest message, Object writer);

  public static native UngroupRequest.ToObjectReturnType toObject(
      boolean includeInstance, UngroupRequest msg);

  public native String addColumnstoungroup(String value, double index);

  public native String addColumnstoungroup(String value);

  public native void clearColumnstoungroupList();

  public native void clearResultid();

  public native void clearSourceid();

  public native JsArray<String> getColumnstoungroupList();

  public native boolean getNullfill();

  public native Ticket getResultid();

  public native TableReference getSourceid();

  public native boolean hasResultid();

  public native boolean hasSourceid();

  public native Uint8Array serializeBinary();

  public native void setColumnstoungroupList(JsArray<String> value);

  @JsOverlay
  public final void setColumnstoungroupList(String[] value) {
    setColumnstoungroupList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setNullfill(boolean value);

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceid();

  public native void setSourceid(TableReference value);

  public native UngroupRequest.ToObjectReturnType0 toObject();

  public native UngroupRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

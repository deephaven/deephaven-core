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
    name = "dhinternal.io.deephaven.proto.table_pb.SelectDistinctRequest",
    namespace = JsPackage.GLOBAL)
public class SelectDistinctRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SelectDistinctRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(
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
      static SelectDistinctRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SelectDistinctRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SelectDistinctRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<SelectDistinctRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<SelectDistinctRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static SelectDistinctRequest.ToObjectReturnType.SourceidFieldType create() {
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
    static SelectDistinctRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnnamesList();

    @JsProperty
    SelectDistinctRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    SelectDistinctRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsProperty
    void setColumnnamesList(JsArray<String> columnnamesList);

    @JsOverlay
    default void setColumnnamesList(String[] columnnamesList) {
      setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
    }

    @JsProperty
    void setResultid(SelectDistinctRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(SelectDistinctRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static SelectDistinctRequest.ToObjectReturnType0.SourceidFieldType create() {
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
    static SelectDistinctRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnnamesList();

    @JsProperty
    SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    SelectDistinctRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsProperty
    void setColumnnamesList(JsArray<String> columnnamesList);

    @JsOverlay
    default void setColumnnamesList(String[] columnnamesList) {
      setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
    }

    @JsProperty
    void setResultid(SelectDistinctRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(SelectDistinctRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static native SelectDistinctRequest deserializeBinary(Uint8Array bytes);

  public static native SelectDistinctRequest deserializeBinaryFromReader(
      SelectDistinctRequest message, Object reader);

  public static native void serializeBinaryToWriter(SelectDistinctRequest message, Object writer);

  public static native SelectDistinctRequest.ToObjectReturnType toObject(
      boolean includeInstance, SelectDistinctRequest msg);

  public native String addColumnnames(String value, double index);

  public native String addColumnnames(String value);

  public native void clearColumnnamesList();

  public native void clearResultid();

  public native void clearSourceid();

  public native JsArray<String> getColumnnamesList();

  public native Ticket getResultid();

  public native TableReference getSourceid();

  public native boolean hasResultid();

  public native boolean hasSourceid();

  public native Uint8Array serializeBinary();

  public native void setColumnnamesList(JsArray<String> value);

  @JsOverlay
  public final void setColumnnamesList(String[] value) {
    setColumnnamesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceid();

  public native void setSourceid(TableReference value);

  public native SelectDistinctRequest.ToObjectReturnType0 toObject();

  public native SelectDistinctRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

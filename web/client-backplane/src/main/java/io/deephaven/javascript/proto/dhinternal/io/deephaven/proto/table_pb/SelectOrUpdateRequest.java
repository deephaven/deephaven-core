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
    name = "dhinternal.io.deephaven.proto.table_pb.SelectOrUpdateRequest",
    namespace = JsPackage.GLOBAL)
public class SelectOrUpdateRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(
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
      static SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static SelectOrUpdateRequest.ToObjectReturnType.SourceidFieldType create() {
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
    static SelectOrUpdateRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnspecsList();

    @JsProperty
    SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    SelectOrUpdateRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsProperty
    void setColumnspecsList(JsArray<String> columnspecsList);

    @JsOverlay
    default void setColumnspecsList(String[] columnspecsList) {
      setColumnspecsList(Js.<JsArray<String>>uncheckedCast(columnspecsList));
    }

    @JsProperty
    void setResultid(SelectOrUpdateRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(SelectOrUpdateRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static SelectOrUpdateRequest.ToObjectReturnType0.SourceidFieldType create() {
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
    static SelectOrUpdateRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnspecsList();

    @JsProperty
    SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    SelectOrUpdateRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsProperty
    void setColumnspecsList(JsArray<String> columnspecsList);

    @JsOverlay
    default void setColumnspecsList(String[] columnspecsList) {
      setColumnspecsList(Js.<JsArray<String>>uncheckedCast(columnspecsList));
    }

    @JsProperty
    void setResultid(SelectOrUpdateRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(SelectOrUpdateRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static native SelectOrUpdateRequest deserializeBinary(Uint8Array bytes);

  public static native SelectOrUpdateRequest deserializeBinaryFromReader(
      SelectOrUpdateRequest message, Object reader);

  public static native void serializeBinaryToWriter(SelectOrUpdateRequest message, Object writer);

  public static native SelectOrUpdateRequest.ToObjectReturnType toObject(
      boolean includeInstance, SelectOrUpdateRequest msg);

  public native String addColumnspecs(String value, double index);

  public native String addColumnspecs(String value);

  public native void clearColumnspecsList();

  public native void clearResultid();

  public native void clearSourceid();

  public native JsArray<String> getColumnspecsList();

  public native Ticket getResultid();

  public native TableReference getSourceid();

  public native boolean hasResultid();

  public native boolean hasSourceid();

  public native Uint8Array serializeBinary();

  public native void setColumnspecsList(JsArray<String> value);

  @JsOverlay
  public final void setColumnspecsList(String[] value) {
    setColumnspecsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceid();

  public native void setSourceid(TableReference value);

  public native SelectOrUpdateRequest.ToObjectReturnType0 toObject();

  public native SelectOrUpdateRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

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
    name = "dhinternal.io.deephaven.proto.table_pb.EmptyTableRequest",
    namespace = JsPackage.GLOBAL)
public class EmptyTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static EmptyTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static EmptyTableRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      EmptyTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(EmptyTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<EmptyTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<EmptyTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static EmptyTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnnamesList();

    @JsProperty
    JsArray<String> getColumntypesList();

    @JsProperty
    EmptyTableRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    String getSize();

    @JsProperty
    void setColumnnamesList(JsArray<String> columnnamesList);

    @JsOverlay
    default void setColumnnamesList(String[] columnnamesList) {
      setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
    }

    @JsProperty
    void setColumntypesList(JsArray<String> columntypesList);

    @JsOverlay
    default void setColumntypesList(String[] columntypesList) {
      setColumntypesList(Js.<JsArray<String>>uncheckedCast(columntypesList));
    }

    @JsProperty
    void setResultid(EmptyTableRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSize(String size);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static EmptyTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static EmptyTableRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      EmptyTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(EmptyTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<EmptyTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<EmptyTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static EmptyTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnnamesList();

    @JsProperty
    JsArray<String> getColumntypesList();

    @JsProperty
    EmptyTableRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    String getSize();

    @JsProperty
    void setColumnnamesList(JsArray<String> columnnamesList);

    @JsOverlay
    default void setColumnnamesList(String[] columnnamesList) {
      setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
    }

    @JsProperty
    void setColumntypesList(JsArray<String> columntypesList);

    @JsOverlay
    default void setColumntypesList(String[] columntypesList) {
      setColumntypesList(Js.<JsArray<String>>uncheckedCast(columntypesList));
    }

    @JsProperty
    void setResultid(EmptyTableRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSize(String size);
  }

  public static native EmptyTableRequest deserializeBinary(Uint8Array bytes);

  public static native EmptyTableRequest deserializeBinaryFromReader(
      EmptyTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(EmptyTableRequest message, Object writer);

  public static native EmptyTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, EmptyTableRequest msg);

  public native String addColumnnames(String value, double index);

  public native String addColumnnames(String value);

  public native String addColumntypes(String value, double index);

  public native String addColumntypes(String value);

  public native void clearColumnnamesList();

  public native void clearColumntypesList();

  public native void clearResultid();

  public native JsArray<String> getColumnnamesList();

  public native JsArray<String> getColumntypesList();

  public native Ticket getResultid();

  public native String getSize();

  public native boolean hasResultid();

  public native Uint8Array serializeBinary();

  public native void setColumnnamesList(JsArray<String> value);

  @JsOverlay
  public final void setColumnnamesList(String[] value) {
    setColumnnamesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setColumntypesList(JsArray<String> value);

  @JsOverlay
  public final void setColumntypesList(String[] value) {
    setColumntypesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSize(String value);

  public native EmptyTableRequest.ToObjectReturnType0 toObject();

  public native EmptyTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

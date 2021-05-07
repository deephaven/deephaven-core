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
    name = "dhinternal.io.deephaven.proto.table_pb.DropColumnsRequest",
    namespace = JsPackage.GLOBAL)
public class DropColumnsRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static DropColumnsRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static DropColumnsRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      DropColumnsRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(DropColumnsRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<DropColumnsRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<DropColumnsRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static DropColumnsRequest.ToObjectReturnType.SourceidFieldType create() {
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
    static DropColumnsRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnnamesList();

    @JsProperty
    DropColumnsRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    DropColumnsRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsProperty
    void setColumnnamesList(JsArray<String> columnnamesList);

    @JsOverlay
    default void setColumnnamesList(String[] columnnamesList) {
      setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
    }

    @JsProperty
    void setResultid(DropColumnsRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(DropColumnsRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static DropColumnsRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static DropColumnsRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      DropColumnsRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(DropColumnsRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<DropColumnsRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<DropColumnsRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static DropColumnsRequest.ToObjectReturnType0.SourceidFieldType create() {
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
    static DropColumnsRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnnamesList();

    @JsProperty
    DropColumnsRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    DropColumnsRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsProperty
    void setColumnnamesList(JsArray<String> columnnamesList);

    @JsOverlay
    default void setColumnnamesList(String[] columnnamesList) {
      setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
    }

    @JsProperty
    void setResultid(DropColumnsRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(DropColumnsRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static native DropColumnsRequest deserializeBinary(Uint8Array bytes);

  public static native DropColumnsRequest deserializeBinaryFromReader(
      DropColumnsRequest message, Object reader);

  public static native void serializeBinaryToWriter(DropColumnsRequest message, Object writer);

  public static native DropColumnsRequest.ToObjectReturnType toObject(
      boolean includeInstance, DropColumnsRequest msg);

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

  public native DropColumnsRequest.ToObjectReturnType0 toObject();

  public native DropColumnsRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

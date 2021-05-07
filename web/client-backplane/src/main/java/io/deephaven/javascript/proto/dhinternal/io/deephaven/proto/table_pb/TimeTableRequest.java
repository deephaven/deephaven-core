package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

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
    name = "dhinternal.io.deephaven.proto.table_pb.TimeTableRequest",
    namespace = JsPackage.GLOBAL)
public class TimeTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static TimeTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static TimeTableRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      TimeTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(TimeTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static TimeTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getPeriodnanos();

    @JsProperty
    TimeTableRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    String getStarttimenanos();

    @JsProperty
    void setPeriodnanos(String periodnanos);

    @JsProperty
    void setResultid(TimeTableRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setStarttimenanos(String starttimenanos);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static TimeTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(Object o) {
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
      static TimeTableRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      TimeTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(TimeTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static TimeTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getPeriodnanos();

    @JsProperty
    TimeTableRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    String getStarttimenanos();

    @JsProperty
    void setPeriodnanos(String periodnanos);

    @JsProperty
    void setResultid(TimeTableRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setStarttimenanos(String starttimenanos);
  }

  public static native TimeTableRequest deserializeBinary(Uint8Array bytes);

  public static native TimeTableRequest deserializeBinaryFromReader(
      TimeTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(TimeTableRequest message, Object writer);

  public static native TimeTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, TimeTableRequest msg);

  public native void clearResultid();

  public native String getPeriodnanos();

  public native Ticket getResultid();

  public native String getStarttimenanos();

  public native boolean hasResultid();

  public native Uint8Array serializeBinary();

  public native void setPeriodnanos(String value);

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setStarttimenanos(String value);

  public native TimeTableRequest.ToObjectReturnType0 toObject();

  public native TimeTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

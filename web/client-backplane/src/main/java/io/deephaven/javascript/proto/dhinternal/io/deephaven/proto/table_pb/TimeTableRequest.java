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
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static TimeTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType of(Object o) {
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
      static TimeTableRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      TimeTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(TimeTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static TimeTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getPeriodNanos();

    @JsProperty
    TimeTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    String getStartTimeNanos();

    @JsProperty
    void setPeriodNanos(String periodNanos);

    @JsProperty
    void setResultId(TimeTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setStartTimeNanos(String startTimeNanos);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static TimeTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType of(Object o) {
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
      static TimeTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      TimeTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(TimeTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<TimeTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static TimeTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getPeriodNanos();

    @JsProperty
    TimeTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    String getStartTimeNanos();

    @JsProperty
    void setPeriodNanos(String periodNanos);

    @JsProperty
    void setResultId(TimeTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setStartTimeNanos(String startTimeNanos);
  }

  public static native TimeTableRequest deserializeBinary(Uint8Array bytes);

  public static native TimeTableRequest deserializeBinaryFromReader(
      TimeTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(TimeTableRequest message, Object writer);

  public static native TimeTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, TimeTableRequest msg);

  public native void clearResultId();

  public native String getPeriodNanos();

  public native Ticket getResultId();

  public native String getStartTimeNanos();

  public native boolean hasResultId();

  public native Uint8Array serializeBinary();

  public native void setPeriodNanos(String value);

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setStartTimeNanos(String value);

  public native TimeTableRequest.ToObjectReturnType0 toObject();

  public native TimeTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

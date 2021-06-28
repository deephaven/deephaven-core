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
    name = "dhinternal.io.deephaven.proto.table_pb.EmptyTableRequest",
    namespace = JsPackage.GLOBAL)
public class EmptyTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static EmptyTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType of(Object o) {
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
      static EmptyTableRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      EmptyTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(EmptyTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js.<EmptyTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>uncheckedCast(
                id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js.<EmptyTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>uncheckedCast(
                id));
      }
    }

    @JsOverlay
    static EmptyTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    EmptyTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    String getSize();

    @JsProperty
    void setResultId(EmptyTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setSize(String size);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType of(Object o) {
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
      static EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static EmptyTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    String getSize();

    @JsProperty
    void setResultId(EmptyTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setSize(String size);
  }

  public static native EmptyTableRequest deserializeBinary(Uint8Array bytes);

  public static native EmptyTableRequest deserializeBinaryFromReader(
      EmptyTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(EmptyTableRequest message, Object writer);

  public static native EmptyTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, EmptyTableRequest msg);

  public native void clearResultId();

  public native Ticket getResultId();

  public native String getSize();

  public native boolean hasResultId();

  public native Uint8Array serializeBinary();

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setSize(String value);

  public native EmptyTableRequest.ToObjectReturnType0 toObject();

  public native EmptyTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

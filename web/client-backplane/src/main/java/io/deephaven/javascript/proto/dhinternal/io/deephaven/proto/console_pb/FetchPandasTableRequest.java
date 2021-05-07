package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
    name = "dhinternal.io.deephaven.proto.console_pb.FetchPandasTableRequest",
    namespace = JsPackage.GLOBAL)
public class FetchPandasTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(
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
      static FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FetchPandasTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    Object getTableid();

    @JsProperty
    void setConsoleid(FetchPandasTableRequest.ToObjectReturnType.ConsoleidFieldType consoleid);

    @JsProperty
    void setTableid(Object tableid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
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
      static FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FetchPandasTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    Object getTableid();

    @JsProperty
    void setConsoleid(FetchPandasTableRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);

    @JsProperty
    void setTableid(Object tableid);
  }

  public static native FetchPandasTableRequest deserializeBinary(Uint8Array bytes);

  public static native FetchPandasTableRequest deserializeBinaryFromReader(
      FetchPandasTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(FetchPandasTableRequest message, Object writer);

  public static native FetchPandasTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, FetchPandasTableRequest msg);

  public native void clearConsoleid();

  public native void clearTableid();

  public native Ticket getConsoleid();

  public native Ticket getTableid();

  public native boolean hasConsoleid();

  public native boolean hasTableid();

  public native Uint8Array serializeBinary();

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native void setTableid();

  public native void setTableid(Ticket value);

  public native FetchPandasTableRequest.ToObjectReturnType0 toObject();

  public native FetchPandasTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

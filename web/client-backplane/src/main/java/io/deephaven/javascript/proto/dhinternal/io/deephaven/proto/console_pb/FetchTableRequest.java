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
    name = "dhinternal.io.deephaven.proto.console_pb.FetchTableRequest",
    namespace = JsPackage.GLOBAL)
public class FetchTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FetchTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(Object o) {
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
      static FetchTableRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FetchTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FetchTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FetchTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchTableRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    Object getTableid();

    @JsProperty
    String getTablename();

    @JsProperty
    void setConsoleid(FetchTableRequest.ToObjectReturnType.ConsoleidFieldType consoleid);

    @JsProperty
    void setTableid(Object tableid);

    @JsProperty
    void setTablename(String tablename);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
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
      static FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FetchTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    Object getTableid();

    @JsProperty
    String getTablename();

    @JsProperty
    void setConsoleid(FetchTableRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);

    @JsProperty
    void setTableid(Object tableid);

    @JsProperty
    void setTablename(String tablename);
  }

  public static native FetchTableRequest deserializeBinary(Uint8Array bytes);

  public static native FetchTableRequest deserializeBinaryFromReader(
      FetchTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(FetchTableRequest message, Object writer);

  public static native FetchTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, FetchTableRequest msg);

  public native void clearConsoleid();

  public native void clearTableid();

  public native Ticket getConsoleid();

  public native Ticket getTableid();

  public native String getTablename();

  public native boolean hasConsoleid();

  public native boolean hasTableid();

  public native Uint8Array serializeBinary();

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native void setTableid();

  public native void setTableid(Ticket value);

  public native void setTablename(String value);

  public native FetchTableRequest.ToObjectReturnType0 toObject();

  public native FetchTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

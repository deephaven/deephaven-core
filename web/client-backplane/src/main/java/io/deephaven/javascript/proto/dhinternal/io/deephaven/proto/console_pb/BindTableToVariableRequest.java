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
    name = "dhinternal.io.deephaven.proto.console_pb.BindTableToVariableRequest",
    namespace = JsPackage.GLOBAL)
public class BindTableToVariableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(
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
      static BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static BindTableToVariableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    Object getTableid();

    @JsProperty
    String getVariablename();

    @JsProperty
    void setConsoleid(BindTableToVariableRequest.ToObjectReturnType.ConsoleidFieldType consoleid);

    @JsProperty
    void setTableid(Object tableid);

    @JsProperty
    void setVariablename(String variablename);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
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
      static BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static BindTableToVariableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    Object getTableid();

    @JsProperty
    String getVariablename();

    @JsProperty
    void setConsoleid(BindTableToVariableRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);

    @JsProperty
    void setTableid(Object tableid);

    @JsProperty
    void setVariablename(String variablename);
  }

  public static native BindTableToVariableRequest deserializeBinary(Uint8Array bytes);

  public static native BindTableToVariableRequest deserializeBinaryFromReader(
      BindTableToVariableRequest message, Object reader);

  public static native void serializeBinaryToWriter(
      BindTableToVariableRequest message, Object writer);

  public static native BindTableToVariableRequest.ToObjectReturnType toObject(
      boolean includeInstance, BindTableToVariableRequest msg);

  public native void clearConsoleid();

  public native void clearTableid();

  public native Ticket getConsoleid();

  public native Ticket getTableid();

  public native String getVariablename();

  public native boolean hasConsoleid();

  public native boolean hasTableid();

  public native Uint8Array serializeBinary();

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native void setTableid();

  public native void setTableid(Ticket value);

  public native void setVariablename(String value);

  public native BindTableToVariableRequest.ToObjectReturnType0 toObject();

  public native BindTableToVariableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

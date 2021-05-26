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
    name = "dhinternal.io.deephaven.proto.table_pb.ExportedTableUpdateMessage",
    namespace = JsPackage.GLOBAL)
public class ExportedTableUpdateMessage {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ExportIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType.GetIdUnionType of(
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
      static ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static ExportedTableUpdateMessage.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType getExportId();

    @JsProperty
    String getSize();

    @JsProperty
    String getUpdateFailureMessage();

    @JsProperty
    void setExportId(ExportedTableUpdateMessage.ToObjectReturnType.ExportIdFieldType exportId);

    @JsProperty
    void setSize(String size);

    @JsProperty
    void setUpdateFailureMessage(String updateFailureMessage);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ExportIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType.GetIdUnionType of(
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
      static ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static ExportedTableUpdateMessage.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType getExportId();

    @JsProperty
    String getSize();

    @JsProperty
    String getUpdateFailureMessage();

    @JsProperty
    void setExportId(ExportedTableUpdateMessage.ToObjectReturnType0.ExportIdFieldType exportId);

    @JsProperty
    void setSize(String size);

    @JsProperty
    void setUpdateFailureMessage(String updateFailureMessage);
  }

  public static native ExportedTableUpdateMessage deserializeBinary(Uint8Array bytes);

  public static native ExportedTableUpdateMessage deserializeBinaryFromReader(
      ExportedTableUpdateMessage message, Object reader);

  public static native void serializeBinaryToWriter(
      ExportedTableUpdateMessage message, Object writer);

  public static native ExportedTableUpdateMessage.ToObjectReturnType toObject(
      boolean includeInstance, ExportedTableUpdateMessage msg);

  public native void clearExportId();

  public native Ticket getExportId();

  public native String getSize();

  public native String getUpdateFailureMessage();

  public native boolean hasExportId();

  public native Uint8Array serializeBinary();

  public native void setExportId();

  public native void setExportId(Ticket value);

  public native void setSize(String value);

  public native void setUpdateFailureMessage(String value);

  public native ExportedTableUpdateMessage.ToObjectReturnType0 toObject();

  public native ExportedTableUpdateMessage.ToObjectReturnType0 toObject(boolean includeInstance);
}

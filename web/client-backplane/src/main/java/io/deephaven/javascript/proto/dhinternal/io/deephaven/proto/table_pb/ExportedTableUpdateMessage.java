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
    public interface ExportidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType.GetIdUnionType of(
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
      static ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static ExportedTableUpdateMessage.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType getExportid();

    @JsProperty
    String getSize();

    @JsProperty
    String getUpdatefailuremessage();

    @JsProperty
    void setExportid(ExportedTableUpdateMessage.ToObjectReturnType.ExportidFieldType exportid);

    @JsProperty
    void setSize(String size);

    @JsProperty
    void setUpdatefailuremessage(String updatefailuremessage);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ExportidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType.GetIdUnionType of(
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
      static ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static ExportedTableUpdateMessage.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType getExportid();

    @JsProperty
    String getSize();

    @JsProperty
    String getUpdatefailuremessage();

    @JsProperty
    void setExportid(ExportedTableUpdateMessage.ToObjectReturnType0.ExportidFieldType exportid);

    @JsProperty
    void setSize(String size);

    @JsProperty
    void setUpdatefailuremessage(String updatefailuremessage);
  }

  public static native ExportedTableUpdateMessage deserializeBinary(Uint8Array bytes);

  public static native ExportedTableUpdateMessage deserializeBinaryFromReader(
      ExportedTableUpdateMessage message, Object reader);

  public static native void serializeBinaryToWriter(
      ExportedTableUpdateMessage message, Object writer);

  public static native ExportedTableUpdateMessage.ToObjectReturnType toObject(
      boolean includeInstance, ExportedTableUpdateMessage msg);

  public native void clearExportid();

  public native Ticket getExportid();

  public native String getSize();

  public native String getUpdatefailuremessage();

  public native boolean hasExportid();

  public native Uint8Array serializeBinary();

  public native void setExportid();

  public native void setExportid(Ticket value);

  public native void setSize(String value);

  public native void setUpdatefailuremessage(String value);

  public native ExportedTableUpdateMessage.ToObjectReturnType0 toObject();

  public native ExportedTableUpdateMessage.ToObjectReturnType0 toObject(boolean includeInstance);
}

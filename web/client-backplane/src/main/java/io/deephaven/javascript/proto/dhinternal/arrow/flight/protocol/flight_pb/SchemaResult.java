package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb.SchemaResult",
    namespace = JsPackage.GLOBAL)
public class SchemaResult {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface GetSchemaUnionType {
    @JsOverlay
    static SchemaResult.GetSchemaUnionType of(Object o) {
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

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface SetSchemaValueUnionType {
    @JsOverlay
    static SchemaResult.SetSchemaValueUnionType of(Object o) {
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

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaUnionType {
      @JsOverlay
      static SchemaResult.ToObjectReturnType.GetSchemaUnionType of(Object o) {
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
    static SchemaResult.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SchemaResult.ToObjectReturnType.GetSchemaUnionType getSchema();

    @JsProperty
    void setSchema(SchemaResult.ToObjectReturnType.GetSchemaUnionType schema);

    @JsOverlay
    default void setSchema(String schema) {
      setSchema(Js.<SchemaResult.ToObjectReturnType.GetSchemaUnionType>uncheckedCast(schema));
    }

    @JsOverlay
    default void setSchema(Uint8Array schema) {
      setSchema(Js.<SchemaResult.ToObjectReturnType.GetSchemaUnionType>uncheckedCast(schema));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaUnionType {
      @JsOverlay
      static SchemaResult.ToObjectReturnType0.GetSchemaUnionType of(Object o) {
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
    static SchemaResult.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SchemaResult.ToObjectReturnType0.GetSchemaUnionType getSchema();

    @JsProperty
    void setSchema(SchemaResult.ToObjectReturnType0.GetSchemaUnionType schema);

    @JsOverlay
    default void setSchema(String schema) {
      setSchema(Js.<SchemaResult.ToObjectReturnType0.GetSchemaUnionType>uncheckedCast(schema));
    }

    @JsOverlay
    default void setSchema(Uint8Array schema) {
      setSchema(Js.<SchemaResult.ToObjectReturnType0.GetSchemaUnionType>uncheckedCast(schema));
    }
  }

  public static native SchemaResult deserializeBinary(Uint8Array bytes);

  public static native SchemaResult deserializeBinaryFromReader(
      SchemaResult message, Object reader);

  public static native void serializeBinaryToWriter(SchemaResult message, Object writer);

  public static native SchemaResult.ToObjectReturnType toObject(
      boolean includeInstance, SchemaResult msg);

  public native SchemaResult.GetSchemaUnionType getSchema();

  public native String getSchema_asB64();

  public native Uint8Array getSchema_asU8();

  public native Uint8Array serializeBinary();

  public native void setSchema(SchemaResult.SetSchemaValueUnionType value);

  @JsOverlay
  public final void setSchema(String value) {
    setSchema(Js.<SchemaResult.SetSchemaValueUnionType>uncheckedCast(value));
  }

  @JsOverlay
  public final void setSchema(Uint8Array value) {
    setSchema(Js.<SchemaResult.SetSchemaValueUnionType>uncheckedCast(value));
  }

  public native SchemaResult.ToObjectReturnType0 toObject();

  public native SchemaResult.ToObjectReturnType0 toObject(boolean includeInstance);
}

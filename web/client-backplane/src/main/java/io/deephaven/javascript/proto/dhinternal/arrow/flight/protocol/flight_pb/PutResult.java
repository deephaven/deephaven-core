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
    name = "dhinternal.arrow.flight.protocol.Flight_pb.PutResult",
    namespace = JsPackage.GLOBAL)
public class PutResult {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface GetAppMetadataUnionType {
    @JsOverlay
    static PutResult.GetAppMetadataUnionType of(Object o) {
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
  public interface SetAppMetadataValueUnionType {
    @JsOverlay
    static PutResult.SetAppMetadataValueUnionType of(Object o) {
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
    public interface GetAppMetadataUnionType {
      @JsOverlay
      static PutResult.ToObjectReturnType.GetAppMetadataUnionType of(Object o) {
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
    static PutResult.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    PutResult.ToObjectReturnType.GetAppMetadataUnionType getAppMetadata();

    @JsProperty
    void setAppMetadata(PutResult.ToObjectReturnType.GetAppMetadataUnionType appMetadata);

    @JsOverlay
    default void setAppMetadata(String appMetadata) {
      setAppMetadata(
          Js.<PutResult.ToObjectReturnType.GetAppMetadataUnionType>uncheckedCast(appMetadata));
    }

    @JsOverlay
    default void setAppMetadata(Uint8Array appMetadata) {
      setAppMetadata(
          Js.<PutResult.ToObjectReturnType.GetAppMetadataUnionType>uncheckedCast(appMetadata));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetAppMetadataUnionType {
      @JsOverlay
      static PutResult.ToObjectReturnType0.GetAppMetadataUnionType of(Object o) {
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
    static PutResult.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    PutResult.ToObjectReturnType0.GetAppMetadataUnionType getAppMetadata();

    @JsProperty
    void setAppMetadata(PutResult.ToObjectReturnType0.GetAppMetadataUnionType appMetadata);

    @JsOverlay
    default void setAppMetadata(String appMetadata) {
      setAppMetadata(
          Js.<PutResult.ToObjectReturnType0.GetAppMetadataUnionType>uncheckedCast(appMetadata));
    }

    @JsOverlay
    default void setAppMetadata(Uint8Array appMetadata) {
      setAppMetadata(
          Js.<PutResult.ToObjectReturnType0.GetAppMetadataUnionType>uncheckedCast(appMetadata));
    }
  }

  public static native PutResult deserializeBinary(Uint8Array bytes);

  public static native PutResult deserializeBinaryFromReader(PutResult message, Object reader);

  public static native void serializeBinaryToWriter(PutResult message, Object writer);

  public static native PutResult.ToObjectReturnType toObject(
      boolean includeInstance, PutResult msg);

  public native PutResult.GetAppMetadataUnionType getAppMetadata();

  public native String getAppMetadata_asB64();

  public native Uint8Array getAppMetadata_asU8();

  public native Uint8Array serializeBinary();

  public native void setAppMetadata(PutResult.SetAppMetadataValueUnionType value);

  @JsOverlay
  public final void setAppMetadata(String value) {
    setAppMetadata(Js.<PutResult.SetAppMetadataValueUnionType>uncheckedCast(value));
  }

  @JsOverlay
  public final void setAppMetadata(Uint8Array value) {
    setAppMetadata(Js.<PutResult.SetAppMetadataValueUnionType>uncheckedCast(value));
  }

  public native PutResult.ToObjectReturnType0 toObject();

  public native PutResult.ToObjectReturnType0 toObject(boolean includeInstance);
}

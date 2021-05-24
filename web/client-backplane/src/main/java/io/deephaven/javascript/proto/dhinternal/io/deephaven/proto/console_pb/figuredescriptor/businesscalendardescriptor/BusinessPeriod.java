package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod",
    namespace = JsPackage.GLOBAL)
public class BusinessPeriod {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static BusinessPeriod.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getClose();

    @JsProperty
    String getOpen();

    @JsProperty
    void setClose(String close);

    @JsProperty
    void setOpen(String open);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static BusinessPeriod.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getClose();

    @JsProperty
    String getOpen();

    @JsProperty
    void setClose(String close);

    @JsProperty
    void setOpen(String open);
  }

  public static native BusinessPeriod deserializeBinary(Uint8Array bytes);

  public static native BusinessPeriod deserializeBinaryFromReader(
      BusinessPeriod message, Object reader);

  public static native void serializeBinaryToWriter(BusinessPeriod message, Object writer);

  public static native BusinessPeriod.ToObjectReturnType toObject(
      boolean includeInstance, BusinessPeriod msg);

  public native String getClose();

  public native String getOpen();

  public native Uint8Array serializeBinary();

  public native void setClose(String value);

  public native void setOpen(String value);

  public native BusinessPeriod.ToObjectReturnType0 toObject();

  public native BusinessPeriod.ToObjectReturnType0 toObject(boolean includeInstance);
}

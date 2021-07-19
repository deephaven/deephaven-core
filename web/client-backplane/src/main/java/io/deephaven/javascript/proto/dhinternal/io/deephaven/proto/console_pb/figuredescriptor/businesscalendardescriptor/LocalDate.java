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
        "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.BusinessCalendarDescriptor.LocalDate",
    namespace = JsPackage.GLOBAL)
public class LocalDate {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static LocalDate.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDay();

    @JsProperty
    double getMonth();

    @JsProperty
    double getYear();

    @JsProperty
    void setDay(double day);

    @JsProperty
    void setMonth(double month);

    @JsProperty
    void setYear(double year);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static LocalDate.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDay();

    @JsProperty
    double getMonth();

    @JsProperty
    double getYear();

    @JsProperty
    void setDay(double day);

    @JsProperty
    void setMonth(double month);

    @JsProperty
    void setYear(double year);
  }

  public static native LocalDate deserializeBinary(Uint8Array bytes);

  public static native LocalDate deserializeBinaryFromReader(LocalDate message, Object reader);

  public static native void serializeBinaryToWriter(LocalDate message, Object writer);

  public static native LocalDate.ToObjectReturnType toObject(
      boolean includeInstance, LocalDate msg);

  public native int getDay();

  public native int getMonth();

  public native int getYear();

  public native Uint8Array serializeBinary();

  public native void setDay(int value);

  public native void setMonth(int value);

  public native void setYear(int value);

  public native LocalDate.ToObjectReturnType0 toObject();

  public native LocalDate.ToObjectReturnType0 toObject(boolean includeInstance);
}

package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor;

import elemental2.core.JsArray;
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
        "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.BusinessCalendarDescriptor.Holiday",
    namespace = JsPackage.GLOBAL)
public class Holiday {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface BusinessperiodsListFieldType {
      @JsOverlay
      static Holiday.ToObjectReturnType.BusinessperiodsListFieldType create() {
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
    public interface DateFieldType {
      @JsOverlay
      static Holiday.ToObjectReturnType.DateFieldType create() {
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

    @JsOverlay
    static Holiday.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Holiday.ToObjectReturnType.BusinessperiodsListFieldType> getBusinessperiodsList();

    @JsProperty
    Holiday.ToObjectReturnType.DateFieldType getDate();

    @JsOverlay
    default void setBusinessperiodsList(
        Holiday.ToObjectReturnType.BusinessperiodsListFieldType[] businessperiodsList) {
      setBusinessperiodsList(
          Js.<JsArray<Holiday.ToObjectReturnType.BusinessperiodsListFieldType>>uncheckedCast(
              businessperiodsList));
    }

    @JsProperty
    void setBusinessperiodsList(
        JsArray<Holiday.ToObjectReturnType.BusinessperiodsListFieldType> businessperiodsList);

    @JsProperty
    void setDate(Holiday.ToObjectReturnType.DateFieldType date);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface BusinessperiodsListFieldType {
      @JsOverlay
      static Holiday.ToObjectReturnType0.BusinessperiodsListFieldType create() {
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
    public interface DateFieldType {
      @JsOverlay
      static Holiday.ToObjectReturnType0.DateFieldType create() {
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

    @JsOverlay
    static Holiday.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Holiday.ToObjectReturnType0.BusinessperiodsListFieldType> getBusinessperiodsList();

    @JsProperty
    Holiday.ToObjectReturnType0.DateFieldType getDate();

    @JsOverlay
    default void setBusinessperiodsList(
        Holiday.ToObjectReturnType0.BusinessperiodsListFieldType[] businessperiodsList) {
      setBusinessperiodsList(
          Js.<JsArray<Holiday.ToObjectReturnType0.BusinessperiodsListFieldType>>uncheckedCast(
              businessperiodsList));
    }

    @JsProperty
    void setBusinessperiodsList(
        JsArray<Holiday.ToObjectReturnType0.BusinessperiodsListFieldType> businessperiodsList);

    @JsProperty
    void setDate(Holiday.ToObjectReturnType0.DateFieldType date);
  }

  public static native Holiday deserializeBinary(Uint8Array bytes);

  public static native Holiday deserializeBinaryFromReader(Holiday message, Object reader);

  public static native void serializeBinaryToWriter(Holiday message, Object writer);

  public static native Holiday.ToObjectReturnType toObject(boolean includeInstance, Holiday msg);

  public native BusinessPeriod addBusinessperiods();

  public native BusinessPeriod addBusinessperiods(BusinessPeriod value, double index);

  public native BusinessPeriod addBusinessperiods(BusinessPeriod value);

  public native void clearBusinessperiodsList();

  public native void clearDate();

  public native JsArray<BusinessPeriod> getBusinessperiodsList();

  public native LocalDate getDate();

  public native boolean hasDate();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setBusinessperiodsList(BusinessPeriod[] value) {
    setBusinessperiodsList(Js.<JsArray<BusinessPeriod>>uncheckedCast(value));
  }

  public native void setBusinessperiodsList(JsArray<BusinessPeriod> value);

  public native void setDate();

  public native void setDate(LocalDate value);

  public native Holiday.ToObjectReturnType0 toObject();

  public native Holiday.ToObjectReturnType0 toObject(boolean includeInstance);
}

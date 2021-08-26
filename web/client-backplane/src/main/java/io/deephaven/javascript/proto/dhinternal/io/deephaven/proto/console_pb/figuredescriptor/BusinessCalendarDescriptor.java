package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor.BusinessPeriod;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor.DayOfWeekMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.businesscalendardescriptor.Holiday;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.BusinessCalendarDescriptor",
        namespace = JsPackage.GLOBAL)
public class BusinessCalendarDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BusinessPeriodsListFieldType {
            @JsOverlay
            static BusinessCalendarDescriptor.ToObjectReturnType.BusinessPeriodsListFieldType create() {
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
        public interface HolidaysListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DateFieldType {
                @JsOverlay
                static BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType.DateFieldType create() {
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
            static BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Object> getBusinessPeriodsList();

            @JsProperty
            BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType.DateFieldType getDate();

            @JsProperty
            void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

            @JsOverlay
            default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
            }

            @JsProperty
            void setDate(
                    BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType.DateFieldType date);
        }

        @JsOverlay
        static BusinessCalendarDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Double> getBusinessDaysList();

        @JsProperty
        JsArray<BusinessCalendarDescriptor.ToObjectReturnType.BusinessPeriodsListFieldType> getBusinessPeriodsList();

        @JsProperty
        JsArray<BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType> getHolidaysList();

        @JsProperty
        String getName();

        @JsProperty
        String getTimeZone();

        @JsProperty
        void setBusinessDaysList(JsArray<Double> businessDaysList);

        @JsOverlay
        default void setBusinessDaysList(double[] businessDaysList) {
            setBusinessDaysList(Js.<JsArray<Double>>uncheckedCast(businessDaysList));
        }

        @JsOverlay
        default void setBusinessPeriodsList(
                BusinessCalendarDescriptor.ToObjectReturnType.BusinessPeriodsListFieldType[] businessPeriodsList) {
            setBusinessPeriodsList(
                    Js.<JsArray<BusinessCalendarDescriptor.ToObjectReturnType.BusinessPeriodsListFieldType>>uncheckedCast(
                            businessPeriodsList));
        }

        @JsProperty
        void setBusinessPeriodsList(
                JsArray<BusinessCalendarDescriptor.ToObjectReturnType.BusinessPeriodsListFieldType> businessPeriodsList);

        @JsOverlay
        default void setHolidaysList(
                BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType[] holidaysList) {
            setHolidaysList(
                    Js.<JsArray<BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType>>uncheckedCast(
                            holidaysList));
        }

        @JsProperty
        void setHolidaysList(
                JsArray<BusinessCalendarDescriptor.ToObjectReturnType.HolidaysListFieldType> holidaysList);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setTimeZone(String timeZone);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BusinessPeriodsListFieldType {
            @JsOverlay
            static BusinessCalendarDescriptor.ToObjectReturnType0.BusinessPeriodsListFieldType create() {
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
        public interface HolidaysListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DateFieldType {
                @JsOverlay
                static BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType.DateFieldType create() {
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
            static BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Object> getBusinessPeriodsList();

            @JsProperty
            BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType.DateFieldType getDate();

            @JsProperty
            void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

            @JsOverlay
            default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
            }

            @JsProperty
            void setDate(
                    BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType.DateFieldType date);
        }

        @JsOverlay
        static BusinessCalendarDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Double> getBusinessDaysList();

        @JsProperty
        JsArray<BusinessCalendarDescriptor.ToObjectReturnType0.BusinessPeriodsListFieldType> getBusinessPeriodsList();

        @JsProperty
        JsArray<BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType> getHolidaysList();

        @JsProperty
        String getName();

        @JsProperty
        String getTimeZone();

        @JsProperty
        void setBusinessDaysList(JsArray<Double> businessDaysList);

        @JsOverlay
        default void setBusinessDaysList(double[] businessDaysList) {
            setBusinessDaysList(Js.<JsArray<Double>>uncheckedCast(businessDaysList));
        }

        @JsOverlay
        default void setBusinessPeriodsList(
                BusinessCalendarDescriptor.ToObjectReturnType0.BusinessPeriodsListFieldType[] businessPeriodsList) {
            setBusinessPeriodsList(
                    Js.<JsArray<BusinessCalendarDescriptor.ToObjectReturnType0.BusinessPeriodsListFieldType>>uncheckedCast(
                            businessPeriodsList));
        }

        @JsProperty
        void setBusinessPeriodsList(
                JsArray<BusinessCalendarDescriptor.ToObjectReturnType0.BusinessPeriodsListFieldType> businessPeriodsList);

        @JsOverlay
        default void setHolidaysList(
                BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType[] holidaysList) {
            setHolidaysList(
                    Js.<JsArray<BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType>>uncheckedCast(
                            holidaysList));
        }

        @JsProperty
        void setHolidaysList(
                JsArray<BusinessCalendarDescriptor.ToObjectReturnType0.HolidaysListFieldType> holidaysList);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setTimeZone(String timeZone);
    }

    public static DayOfWeekMap DayOfWeek;

    public static native BusinessCalendarDescriptor deserializeBinary(Uint8Array bytes);

    public static native BusinessCalendarDescriptor deserializeBinaryFromReader(
            BusinessCalendarDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(
            BusinessCalendarDescriptor message, Object writer);

    public static native BusinessCalendarDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, BusinessCalendarDescriptor msg);

    public native double addBusinessDays(double value, double index);

    public native double addBusinessDays(double value);

    public native BusinessPeriod addBusinessPeriods();

    public native BusinessPeriod addBusinessPeriods(BusinessPeriod value, double index);

    public native BusinessPeriod addBusinessPeriods(BusinessPeriod value);

    public native Holiday addHolidays();

    public native Holiday addHolidays(Holiday value, double index);

    public native Holiday addHolidays(Holiday value);

    public native void clearBusinessDaysList();

    public native void clearBusinessPeriodsList();

    public native void clearHolidaysList();

    public native JsArray<Double> getBusinessDaysList();

    public native JsArray<BusinessPeriod> getBusinessPeriodsList();

    public native JsArray<Holiday> getHolidaysList();

    public native String getName();

    public native String getTimeZone();

    public native Uint8Array serializeBinary();

    public native void setBusinessDaysList(JsArray<Double> value);

    @JsOverlay
    public final void setBusinessDaysList(double[] value) {
        setBusinessDaysList(Js.<JsArray<Double>>uncheckedCast(value));
    }

    @JsOverlay
    public final void setBusinessPeriodsList(BusinessPeriod[] value) {
        setBusinessPeriodsList(Js.<JsArray<BusinessPeriod>>uncheckedCast(value));
    }

    public native void setBusinessPeriodsList(JsArray<BusinessPeriod> value);

    @JsOverlay
    public final void setHolidaysList(Holiday[] value) {
        setHolidaysList(Js.<JsArray<Holiday>>uncheckedCast(value));
    }

    public native void setHolidaysList(JsArray<Holiday> value);

    public native void setName(String value);

    public native void setTimeZone(String value);

    public native BusinessCalendarDescriptor.ToObjectReturnType0 toObject();

    public native BusinessCalendarDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}

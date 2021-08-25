package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor.AxisFormatTypeMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor.AxisPositionMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor.AxisTypeMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.AxisDescriptor",
        namespace = JsPackage.GLOBAL)
public class AxisDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BusinessCalendarDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BusinessPeriodsListFieldType {
                @JsOverlay
                static AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType create() {
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
                    static AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType create() {
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
                static AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<Object> getBusinessPeriodsList();

                @JsProperty
                AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType getDate();

                @JsProperty
                void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

                @JsOverlay
                default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                    setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
                }

                @JsProperty
                void setDate(
                        AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType date);
            }

            @JsOverlay
            static AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Double> getBusinessDaysList();

            @JsProperty
            JsArray<AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> getBusinessPeriodsList();

            @JsProperty
            JsArray<AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> getHolidaysList();

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
                    AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType[] businessPeriodsList) {
                setBusinessPeriodsList(
                        Js.<JsArray<AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>>uncheckedCast(
                                businessPeriodsList));
            }

            @JsProperty
            void setBusinessPeriodsList(
                    JsArray<AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> businessPeriodsList);

            @JsOverlay
            default void setHolidaysList(
                    AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType[] holidaysList) {
                setHolidaysList(
                        Js.<JsArray<AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>uncheckedCast(
                                holidaysList));
            }

            @JsProperty
            void setHolidaysList(
                    JsArray<AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> holidaysList);

            @JsProperty
            void setName(String name);

            @JsProperty
            void setTimeZone(String timeZone);
        }

        @JsOverlay
        static AxisDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType getBusinessCalendarDescriptor();

        @JsProperty
        String getColor();

        @JsProperty
        String getFormatPattern();

        @JsProperty
        double getFormatType();

        @JsProperty
        double getGapBetweenMajorTicks();

        @JsProperty
        String getId();

        @JsProperty
        String getLabel();

        @JsProperty
        String getLabelFont();

        @JsProperty
        JsArray<Double> getMajorTickLocationsList();

        @JsProperty
        double getMaxRange();

        @JsProperty
        double getMinRange();

        @JsProperty
        double getMinorTickCount();

        @JsProperty
        double getPosition();

        @JsProperty
        double getTickLabelAngle();

        @JsProperty
        String getTicksFont();

        @JsProperty
        double getType();

        @JsProperty
        boolean isInvert();

        @JsProperty
        boolean isIsTimeAxis();

        @JsProperty
        boolean isLog();

        @JsProperty
        boolean isMajorTicksVisible();

        @JsProperty
        boolean isMinorTicksVisible();

        @JsProperty
        void setBusinessCalendarDescriptor(
                AxisDescriptor.ToObjectReturnType.BusinessCalendarDescriptorFieldType businessCalendarDescriptor);

        @JsProperty
        void setColor(String color);

        @JsProperty
        void setFormatPattern(String formatPattern);

        @JsProperty
        void setFormatType(double formatType);

        @JsProperty
        void setGapBetweenMajorTicks(double gapBetweenMajorTicks);

        @JsProperty
        void setId(String id);

        @JsProperty
        void setInvert(boolean invert);

        @JsProperty
        void setIsTimeAxis(boolean isTimeAxis);

        @JsProperty
        void setLabel(String label);

        @JsProperty
        void setLabelFont(String labelFont);

        @JsProperty
        void setLog(boolean log);

        @JsProperty
        void setMajorTickLocationsList(JsArray<Double> majorTickLocationsList);

        @JsOverlay
        default void setMajorTickLocationsList(double[] majorTickLocationsList) {
            setMajorTickLocationsList(Js.<JsArray<Double>>uncheckedCast(majorTickLocationsList));
        }

        @JsProperty
        void setMajorTicksVisible(boolean majorTicksVisible);

        @JsProperty
        void setMaxRange(double maxRange);

        @JsProperty
        void setMinRange(double minRange);

        @JsProperty
        void setMinorTickCount(double minorTickCount);

        @JsProperty
        void setMinorTicksVisible(boolean minorTicksVisible);

        @JsProperty
        void setPosition(double position);

        @JsProperty
        void setTickLabelAngle(double tickLabelAngle);

        @JsProperty
        void setTicksFont(String ticksFont);

        @JsProperty
        void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BusinessCalendarDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BusinessPeriodsListFieldType {
                @JsOverlay
                static AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType create() {
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
                    static AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType create() {
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
                static AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<Object> getBusinessPeriodsList();

                @JsProperty
                AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType getDate();

                @JsProperty
                void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

                @JsOverlay
                default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                    setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
                }

                @JsProperty
                void setDate(
                        AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType date);
            }

            @JsOverlay
            static AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Double> getBusinessDaysList();

            @JsProperty
            JsArray<AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> getBusinessPeriodsList();

            @JsProperty
            JsArray<AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> getHolidaysList();

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
                    AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType[] businessPeriodsList) {
                setBusinessPeriodsList(
                        Js.<JsArray<AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>>uncheckedCast(
                                businessPeriodsList));
            }

            @JsProperty
            void setBusinessPeriodsList(
                    JsArray<AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> businessPeriodsList);

            @JsOverlay
            default void setHolidaysList(
                    AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType[] holidaysList) {
                setHolidaysList(
                        Js.<JsArray<AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>uncheckedCast(
                                holidaysList));
            }

            @JsProperty
            void setHolidaysList(
                    JsArray<AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> holidaysList);

            @JsProperty
            void setName(String name);

            @JsProperty
            void setTimeZone(String timeZone);
        }

        @JsOverlay
        static AxisDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType getBusinessCalendarDescriptor();

        @JsProperty
        String getColor();

        @JsProperty
        String getFormatPattern();

        @JsProperty
        double getFormatType();

        @JsProperty
        double getGapBetweenMajorTicks();

        @JsProperty
        String getId();

        @JsProperty
        String getLabel();

        @JsProperty
        String getLabelFont();

        @JsProperty
        JsArray<Double> getMajorTickLocationsList();

        @JsProperty
        double getMaxRange();

        @JsProperty
        double getMinRange();

        @JsProperty
        double getMinorTickCount();

        @JsProperty
        double getPosition();

        @JsProperty
        double getTickLabelAngle();

        @JsProperty
        String getTicksFont();

        @JsProperty
        double getType();

        @JsProperty
        boolean isInvert();

        @JsProperty
        boolean isIsTimeAxis();

        @JsProperty
        boolean isLog();

        @JsProperty
        boolean isMajorTicksVisible();

        @JsProperty
        boolean isMinorTicksVisible();

        @JsProperty
        void setBusinessCalendarDescriptor(
                AxisDescriptor.ToObjectReturnType0.BusinessCalendarDescriptorFieldType businessCalendarDescriptor);

        @JsProperty
        void setColor(String color);

        @JsProperty
        void setFormatPattern(String formatPattern);

        @JsProperty
        void setFormatType(double formatType);

        @JsProperty
        void setGapBetweenMajorTicks(double gapBetweenMajorTicks);

        @JsProperty
        void setId(String id);

        @JsProperty
        void setInvert(boolean invert);

        @JsProperty
        void setIsTimeAxis(boolean isTimeAxis);

        @JsProperty
        void setLabel(String label);

        @JsProperty
        void setLabelFont(String labelFont);

        @JsProperty
        void setLog(boolean log);

        @JsProperty
        void setMajorTickLocationsList(JsArray<Double> majorTickLocationsList);

        @JsOverlay
        default void setMajorTickLocationsList(double[] majorTickLocationsList) {
            setMajorTickLocationsList(Js.<JsArray<Double>>uncheckedCast(majorTickLocationsList));
        }

        @JsProperty
        void setMajorTicksVisible(boolean majorTicksVisible);

        @JsProperty
        void setMaxRange(double maxRange);

        @JsProperty
        void setMinRange(double minRange);

        @JsProperty
        void setMinorTickCount(double minorTickCount);

        @JsProperty
        void setMinorTicksVisible(boolean minorTicksVisible);

        @JsProperty
        void setPosition(double position);

        @JsProperty
        void setTickLabelAngle(double tickLabelAngle);

        @JsProperty
        void setTicksFont(String ticksFont);

        @JsProperty
        void setType(double type);
    }

    public static AxisFormatTypeMap AxisFormatType;
    public static AxisPositionMap AxisPosition;
    public static AxisTypeMap AxisType;

    public static native AxisDescriptor deserializeBinary(Uint8Array bytes);

    public static native AxisDescriptor deserializeBinaryFromReader(
            AxisDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(AxisDescriptor message, Object writer);

    public static native AxisDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, AxisDescriptor msg);

    public native double addMajorTickLocations(double value, double index);

    public native double addMajorTickLocations(double value);

    public native void clearBusinessCalendarDescriptor();

    public native void clearFormatPattern();

    public native void clearGapBetweenMajorTicks();

    public native void clearMajorTickLocationsList();

    public native BusinessCalendarDescriptor getBusinessCalendarDescriptor();

    public native String getColor();

    public native String getFormatPattern();

    public native int getFormatType();

    public native double getGapBetweenMajorTicks();

    public native String getId();

    public native boolean getInvert();

    public native boolean getIsTimeAxis();

    public native String getLabel();

    public native String getLabelFont();

    public native boolean getLog();

    public native JsArray<Double> getMajorTickLocationsList();

    public native boolean getMajorTicksVisible();

    public native double getMaxRange();

    public native double getMinRange();

    public native int getMinorTickCount();

    public native boolean getMinorTicksVisible();

    public native int getPosition();

    public native double getTickLabelAngle();

    public native String getTicksFont();

    public native int getType();

    public native boolean hasBusinessCalendarDescriptor();

    public native boolean hasFormatPattern();

    public native boolean hasGapBetweenMajorTicks();

    public native Uint8Array serializeBinary();

    public native void setBusinessCalendarDescriptor();

    public native void setBusinessCalendarDescriptor(BusinessCalendarDescriptor value);

    public native void setColor(String value);

    public native void setFormatPattern(String value);

    public native void setFormatType(int value);

    public native void setGapBetweenMajorTicks(double value);

    public native void setId(String value);

    public native void setInvert(boolean value);

    public native void setIsTimeAxis(boolean value);

    public native void setLabel(String value);

    public native void setLabelFont(String value);

    public native void setLog(boolean value);

    public native void setMajorTickLocationsList(JsArray<Double> value);

    @JsOverlay
    public final void setMajorTickLocationsList(double[] value) {
        setMajorTickLocationsList(Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void setMajorTicksVisible(boolean value);

    public native void setMaxRange(double value);

    public native void setMinRange(double value);

    public native void setMinorTickCount(int value);

    public native void setMinorTicksVisible(boolean value);

    public native void setPosition(int value);

    public native void setTickLabelAngle(double value);

    public native void setTicksFont(String value);

    public native void setType(int value);

    public native AxisDescriptor.ToObjectReturnType0 toObject();

    public native AxisDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}

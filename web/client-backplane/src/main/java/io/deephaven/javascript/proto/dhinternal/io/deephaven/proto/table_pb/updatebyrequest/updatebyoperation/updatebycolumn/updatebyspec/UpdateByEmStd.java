package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UpdateByEmOptions;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UpdateByWindowScale;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmStd",
        namespace = JsPackage.GLOBAL)
public class UpdateByEmStd {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BigValueContextFieldType {
                @JsOverlay
                static UpdateByEmStd.ToObjectReturnType.OptionsFieldType.BigValueContextFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getPrecision();

                @JsProperty
                double getRoundingMode();

                @JsProperty
                void setPrecision(double precision);

                @JsProperty
                void setRoundingMode(double roundingMode);
            }

            @JsOverlay
            static UpdateByEmStd.ToObjectReturnType.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEmStd.ToObjectReturnType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

            @JsProperty
            double getOnNanValue();

            @JsProperty
            double getOnNegativeDeltaTime();

            @JsProperty
            double getOnNullTime();

            @JsProperty
            double getOnNullValue();

            @JsProperty
            double getOnZeroDeltaTime();

            @JsProperty
            void setBigValueContext(
                    UpdateByEmStd.ToObjectReturnType.OptionsFieldType.BigValueContextFieldType bigValueContext);

            @JsProperty
            void setOnNanValue(double onNanValue);

            @JsProperty
            void setOnNegativeDeltaTime(double onNegativeDeltaTime);

            @JsProperty
            void setOnNullTime(double onNullTime);

            @JsProperty
            void setOnNullValue(double onNullValue);

            @JsProperty
            void setOnZeroDeltaTime(double onZeroDeltaTime);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface WindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType.TicksFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getTicks();

                @JsProperty
                void setTicks(double ticks);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeFieldType {
                @JsOverlay
                static UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getDurationString();

                @JsProperty
                String getNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setDurationString(String durationString);

                @JsProperty
                void setNanos(String nanos);
            }

            @JsOverlay
            static UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByEmStd.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmStd.ToObjectReturnType.OptionsFieldType getOptions();

        @JsProperty
        UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType getWindowScale();

        @JsProperty
        void setOptions(UpdateByEmStd.ToObjectReturnType.OptionsFieldType options);

        @JsProperty
        void setWindowScale(UpdateByEmStd.ToObjectReturnType.WindowScaleFieldType windowScale);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BigValueContextFieldType {
                @JsOverlay
                static UpdateByEmStd.ToObjectReturnType0.OptionsFieldType.BigValueContextFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getPrecision();

                @JsProperty
                double getRoundingMode();

                @JsProperty
                void setPrecision(double precision);

                @JsProperty
                void setRoundingMode(double roundingMode);
            }

            @JsOverlay
            static UpdateByEmStd.ToObjectReturnType0.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEmStd.ToObjectReturnType0.OptionsFieldType.BigValueContextFieldType getBigValueContext();

            @JsProperty
            double getOnNanValue();

            @JsProperty
            double getOnNegativeDeltaTime();

            @JsProperty
            double getOnNullTime();

            @JsProperty
            double getOnNullValue();

            @JsProperty
            double getOnZeroDeltaTime();

            @JsProperty
            void setBigValueContext(
                    UpdateByEmStd.ToObjectReturnType0.OptionsFieldType.BigValueContextFieldType bigValueContext);

            @JsProperty
            void setOnNanValue(double onNanValue);

            @JsProperty
            void setOnNegativeDeltaTime(double onNegativeDeltaTime);

            @JsProperty
            void setOnNullTime(double onNullTime);

            @JsProperty
            void setOnNullValue(double onNullValue);

            @JsProperty
            void setOnZeroDeltaTime(double onZeroDeltaTime);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface WindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType.TicksFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getTicks();

                @JsProperty
                void setTicks(double ticks);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeFieldType {
                @JsOverlay
                static UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getDurationString();

                @JsProperty
                String getNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setDurationString(String durationString);

                @JsProperty
                void setNanos(String nanos);
            }

            @JsOverlay
            static UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByEmStd.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmStd.ToObjectReturnType0.OptionsFieldType getOptions();

        @JsProperty
        UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType getWindowScale();

        @JsProperty
        void setOptions(UpdateByEmStd.ToObjectReturnType0.OptionsFieldType options);

        @JsProperty
        void setWindowScale(UpdateByEmStd.ToObjectReturnType0.WindowScaleFieldType windowScale);
    }

    public static native UpdateByEmStd deserializeBinary(Uint8Array bytes);

    public static native UpdateByEmStd deserializeBinaryFromReader(
            UpdateByEmStd message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByEmStd message, Object writer);

    public static native UpdateByEmStd.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByEmStd msg);

    public native void clearOptions();

    public native void clearWindowScale();

    public native UpdateByEmOptions getOptions();

    public native UpdateByWindowScale getWindowScale();

    public native boolean hasOptions();

    public native boolean hasWindowScale();

    public native Uint8Array serializeBinary();

    public native void setOptions();

    public native void setOptions(UpdateByEmOptions value);

    public native void setWindowScale();

    public native void setWindowScale(UpdateByWindowScale value);

    public native UpdateByEmStd.ToObjectReturnType0 toObject();

    public native UpdateByEmStd.ToObjectReturnType0 toObject(boolean includeInstance);
}

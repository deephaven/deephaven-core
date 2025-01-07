//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.updatebyema.UpdateByEmaOptions;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.updatebyema.UpdateByEmaTimescale;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma",
        namespace = JsPackage.GLOBAL)
public class UpdateByEma {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BigValueContextFieldType {
                @JsOverlay
                static UpdateByEma.ToObjectReturnType.OptionsFieldType.BigValueContextFieldType create() {
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
            static UpdateByEma.ToObjectReturnType.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEma.ToObjectReturnType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                    UpdateByEma.ToObjectReturnType.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
        public interface TimescaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByEma.ToObjectReturnType.TimescaleFieldType.TicksFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getTicks();

                @JsProperty
                void setTicks(String ticks);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeFieldType {
                @JsOverlay
                static UpdateByEma.ToObjectReturnType.TimescaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getPeriodNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setPeriodNanos(String periodNanos);
            }

            @JsOverlay
            static UpdateByEma.ToObjectReturnType.TimescaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEma.ToObjectReturnType.TimescaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByEma.ToObjectReturnType.TimescaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(UpdateByEma.ToObjectReturnType.TimescaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(UpdateByEma.ToObjectReturnType.TimescaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByEma.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEma.ToObjectReturnType.OptionsFieldType getOptions();

        @JsProperty
        UpdateByEma.ToObjectReturnType.TimescaleFieldType getTimescale();

        @JsProperty
        void setOptions(UpdateByEma.ToObjectReturnType.OptionsFieldType options);

        @JsProperty
        void setTimescale(UpdateByEma.ToObjectReturnType.TimescaleFieldType timescale);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BigValueContextFieldType {
                @JsOverlay
                static UpdateByEma.ToObjectReturnType0.OptionsFieldType.BigValueContextFieldType create() {
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
            static UpdateByEma.ToObjectReturnType0.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEma.ToObjectReturnType0.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                    UpdateByEma.ToObjectReturnType0.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
        public interface TimescaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByEma.ToObjectReturnType0.TimescaleFieldType.TicksFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getTicks();

                @JsProperty
                void setTicks(String ticks);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeFieldType {
                @JsOverlay
                static UpdateByEma.ToObjectReturnType0.TimescaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getPeriodNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setPeriodNanos(String periodNanos);
            }

            @JsOverlay
            static UpdateByEma.ToObjectReturnType0.TimescaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByEma.ToObjectReturnType0.TimescaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByEma.ToObjectReturnType0.TimescaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(UpdateByEma.ToObjectReturnType0.TimescaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(UpdateByEma.ToObjectReturnType0.TimescaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByEma.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEma.ToObjectReturnType0.OptionsFieldType getOptions();

        @JsProperty
        UpdateByEma.ToObjectReturnType0.TimescaleFieldType getTimescale();

        @JsProperty
        void setOptions(UpdateByEma.ToObjectReturnType0.OptionsFieldType options);

        @JsProperty
        void setTimescale(UpdateByEma.ToObjectReturnType0.TimescaleFieldType timescale);
    }

    public static native UpdateByEma deserializeBinary(Uint8Array bytes);

    public static native UpdateByEma deserializeBinaryFromReader(UpdateByEma message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByEma message, Object writer);

    public static native UpdateByEma.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByEma msg);

    public native void clearOptions();

    public native void clearTimescale();

    public native UpdateByEmaOptions getOptions();

    public native UpdateByEmaTimescale getTimescale();

    public native boolean hasOptions();

    public native boolean hasTimescale();

    public native Uint8Array serializeBinary();

    public native void setOptions();

    public native void setOptions(UpdateByEmaOptions value);

    public native void setTimescale();

    public native void setTimescale(UpdateByEmaTimescale value);

    public native UpdateByEma.ToObjectReturnType0 toObject();

    public native UpdateByEma.ToObjectReturnType0 toObject(boolean includeInstance);
}

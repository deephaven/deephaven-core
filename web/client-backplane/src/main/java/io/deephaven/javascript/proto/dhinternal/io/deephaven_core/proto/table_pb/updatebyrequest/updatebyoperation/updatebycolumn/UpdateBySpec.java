//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeMax;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeMin;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeProduct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByEma;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByFill;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec",
        namespace = JsPackage.GLOBAL)
public class UpdateBySpec {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmaFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OptionsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface BigValueContextFieldType {
                    @JsOverlay
                    static UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType.BigValueContextFieldType create() {
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
                static UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                        UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
                    static UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                    static UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                static UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                @JsProperty
                UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                @JsProperty
                void setTicks(
                        UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                @JsProperty
                void setTime(
                        UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
            }

            @JsOverlay
            static UpdateBySpec.ToObjectReturnType.EmaFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType getOptions();

            @JsProperty
            UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType getTimescale();

            @JsProperty
            void setOptions(UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType options);

            @JsProperty
            void setTimescale(UpdateBySpec.ToObjectReturnType.EmaFieldType.TimescaleFieldType timescale);
        }

        @JsOverlay
        static UpdateBySpec.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateBySpec.ToObjectReturnType.EmaFieldType getEma();

        @JsProperty
        Object getFill();

        @JsProperty
        Object getMax();

        @JsProperty
        Object getMin();

        @JsProperty
        Object getProduct();

        @JsProperty
        Object getSum();

        @JsProperty
        void setEma(UpdateBySpec.ToObjectReturnType.EmaFieldType ema);

        @JsProperty
        void setFill(Object fill);

        @JsProperty
        void setMax(Object max);

        @JsProperty
        void setMin(Object min);

        @JsProperty
        void setProduct(Object product);

        @JsProperty
        void setSum(Object sum);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmaFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OptionsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface BigValueContextFieldType {
                    @JsOverlay
                    static UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType.BigValueContextFieldType create() {
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
                static UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                        UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
                    static UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                    static UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                static UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                @JsProperty
                UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                @JsProperty
                void setTicks(
                        UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                @JsProperty
                void setTime(
                        UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType.TimeFieldType time);
            }

            @JsOverlay
            static UpdateBySpec.ToObjectReturnType0.EmaFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType getOptions();

            @JsProperty
            UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType getTimescale();

            @JsProperty
            void setOptions(UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType options);

            @JsProperty
            void setTimescale(UpdateBySpec.ToObjectReturnType0.EmaFieldType.TimescaleFieldType timescale);
        }

        @JsOverlay
        static UpdateBySpec.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.EmaFieldType getEma();

        @JsProperty
        Object getFill();

        @JsProperty
        Object getMax();

        @JsProperty
        Object getMin();

        @JsProperty
        Object getProduct();

        @JsProperty
        Object getSum();

        @JsProperty
        void setEma(UpdateBySpec.ToObjectReturnType0.EmaFieldType ema);

        @JsProperty
        void setFill(Object fill);

        @JsProperty
        void setMax(Object max);

        @JsProperty
        void setMin(Object min);

        @JsProperty
        void setProduct(Object product);

        @JsProperty
        void setSum(Object sum);
    }

    public static native UpdateBySpec deserializeBinary(Uint8Array bytes);

    public static native UpdateBySpec deserializeBinaryFromReader(
            UpdateBySpec message, Object reader);

    public static native void serializeBinaryToWriter(UpdateBySpec message, Object writer);

    public static native UpdateBySpec.ToObjectReturnType toObject(
            boolean includeInstance, UpdateBySpec msg);

    public native void clearEma();

    public native void clearFill();

    public native void clearMax();

    public native void clearMin();

    public native void clearProduct();

    public native void clearSum();

    public native UpdateByEma getEma();

    public native UpdateByFill getFill();

    public native UpdateByCumulativeMax getMax();

    public native UpdateByCumulativeMin getMin();

    public native UpdateByCumulativeProduct getProduct();

    public native UpdateByCumulativeSum getSum();

    public native int getTypeCase();

    public native boolean hasEma();

    public native boolean hasFill();

    public native boolean hasMax();

    public native boolean hasMin();

    public native boolean hasProduct();

    public native boolean hasSum();

    public native Uint8Array serializeBinary();

    public native void setEma();

    public native void setEma(UpdateByEma value);

    public native void setFill();

    public native void setFill(UpdateByFill value);

    public native void setMax();

    public native void setMax(UpdateByCumulativeMax value);

    public native void setMin();

    public native void setMin(UpdateByCumulativeMin value);

    public native void setProduct();

    public native void setProduct(UpdateByCumulativeProduct value);

    public native void setSum();

    public native void setSum(UpdateByCumulativeSum value);

    public native UpdateBySpec.ToObjectReturnType0 toObject();

    public native UpdateBySpec.ToObjectReturnType0 toObject(boolean includeInstance);
}

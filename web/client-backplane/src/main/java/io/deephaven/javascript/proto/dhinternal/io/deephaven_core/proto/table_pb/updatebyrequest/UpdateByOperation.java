//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.UpdateByColumn;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation",
        namespace = JsPackage.GLOBAL)
public class UpdateByOperation {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SpecFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface EmaFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface OptionsFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface BigValueContextFieldType {
                            @JsOverlay
                            static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType create() {
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
                        static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
                            static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                            static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                        static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                        @JsProperty
                        void setTicks(
                                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                        @JsProperty
                        void setTime(
                                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                    }

                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                    @JsProperty
                    void setOptions(
                            UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                    @JsProperty
                    void setTimescale(
                            UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                }

                @JsOverlay
                static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

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
                void setEma(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

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

            @JsOverlay
            static UpdateByOperation.ToObjectReturnType.ColumnFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getMatchPairsList();

            @JsProperty
            UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType getSpec();

            @JsProperty
            void setMatchPairsList(JsArray<String> matchPairsList);

            @JsOverlay
            default void setMatchPairsList(String[] matchPairsList) {
                setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
            }

            @JsProperty
            void setSpec(UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType spec);
        }

        @JsOverlay
        static UpdateByOperation.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByOperation.ToObjectReturnType.ColumnFieldType getColumn();

        @JsProperty
        void setColumn(UpdateByOperation.ToObjectReturnType.ColumnFieldType column);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SpecFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface EmaFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface OptionsFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface BigValueContextFieldType {
                            @JsOverlay
                            static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType create() {
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
                        static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
                            static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                            static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                        static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                        @JsProperty
                        void setTicks(
                                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                        @JsProperty
                        void setTime(
                                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                    }

                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                    @JsProperty
                    void setOptions(
                            UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                    @JsProperty
                    void setTimescale(
                            UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                }

                @JsOverlay
                static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

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
                void setEma(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType ema);

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

            @JsOverlay
            static UpdateByOperation.ToObjectReturnType0.ColumnFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getMatchPairsList();

            @JsProperty
            UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType getSpec();

            @JsProperty
            void setMatchPairsList(JsArray<String> matchPairsList);

            @JsOverlay
            default void setMatchPairsList(String[] matchPairsList) {
                setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
            }

            @JsProperty
            void setSpec(UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType spec);
        }

        @JsOverlay
        static UpdateByOperation.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByOperation.ToObjectReturnType0.ColumnFieldType getColumn();

        @JsProperty
        void setColumn(UpdateByOperation.ToObjectReturnType0.ColumnFieldType column);
    }

    public static native UpdateByOperation deserializeBinary(Uint8Array bytes);

    public static native UpdateByOperation deserializeBinaryFromReader(
            UpdateByOperation message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByOperation message, Object writer);

    public static native UpdateByOperation.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByOperation msg);

    public native void clearColumn();

    public native UpdateByColumn getColumn();

    public native int getTypeCase();

    public native boolean hasColumn();

    public native Uint8Array serializeBinary();

    public native void setColumn();

    public native void setColumn(UpdateByColumn value);

    public native UpdateByOperation.ToObjectReturnType0 toObject();

    public native UpdateByOperation.ToObjectReturnType0 toObject(boolean includeInstance);
}

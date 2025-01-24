//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.UpdateBySpec;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn",
        namespace = JsPackage.GLOBAL)
public class UpdateByColumn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SpecFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmaFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OptionsFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface BigValueContextFieldType {
                        @JsOverlay
                        static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType create() {
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
                    static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
                        static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                        static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                    static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                    @JsProperty
                    void setTicks(
                            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                    @JsProperty
                    void setTime(
                            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                }

                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                @JsProperty
                UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                @JsProperty
                void setOptions(
                        UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                @JsProperty
                void setTimescale(
                        UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
            }

            @JsOverlay
            static UpdateByColumn.ToObjectReturnType.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType getEma();

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
            void setEma(UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType ema);

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
        static UpdateByColumn.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getMatchPairsList();

        @JsProperty
        UpdateByColumn.ToObjectReturnType.SpecFieldType getSpec();

        @JsProperty
        void setMatchPairsList(JsArray<String> matchPairsList);

        @JsOverlay
        default void setMatchPairsList(String[] matchPairsList) {
            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
        }

        @JsProperty
        void setSpec(UpdateByColumn.ToObjectReturnType.SpecFieldType spec);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SpecFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmaFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OptionsFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface BigValueContextFieldType {
                        @JsOverlay
                        static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType create() {
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
                    static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType getBigValueContext();

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
                            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType.BigValueContextFieldType bigValueContext);

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
                        static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                        static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                    static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                    @JsProperty
                    void setTicks(
                            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                    @JsProperty
                    void setTime(
                            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                }

                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                @JsProperty
                UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                @JsProperty
                void setOptions(
                        UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType options);

                @JsProperty
                void setTimescale(
                        UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
            }

            @JsOverlay
            static UpdateByColumn.ToObjectReturnType0.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType getEma();

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
            void setEma(UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType ema);

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
        static UpdateByColumn.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getMatchPairsList();

        @JsProperty
        UpdateByColumn.ToObjectReturnType0.SpecFieldType getSpec();

        @JsProperty
        void setMatchPairsList(JsArray<String> matchPairsList);

        @JsOverlay
        default void setMatchPairsList(String[] matchPairsList) {
            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
        }

        @JsProperty
        void setSpec(UpdateByColumn.ToObjectReturnType0.SpecFieldType spec);
    }

    public static native UpdateByColumn deserializeBinary(Uint8Array bytes);

    public static native UpdateByColumn deserializeBinaryFromReader(
            UpdateByColumn message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByColumn message, Object writer);

    public static native UpdateByColumn.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByColumn msg);

    public native String addMatchPairs(String value, double index);

    public native String addMatchPairs(String value);

    public native void clearMatchPairsList();

    public native void clearSpec();

    public native JsArray<String> getMatchPairsList();

    public native UpdateBySpec getSpec();

    public native boolean hasSpec();

    public native Uint8Array serializeBinary();

    public native void setMatchPairsList(JsArray<String> value);

    @JsOverlay
    public final void setMatchPairsList(String[] value) {
        setMatchPairsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setSpec();

    public native void setSpec(UpdateBySpec value);

    public native UpdateByColumn.ToObjectReturnType0 toObject();

    public native UpdateByColumn.ToObjectReturnType0 toObject(boolean includeInstance);
}

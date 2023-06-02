/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.UpdateBySpec;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn",
        namespace = JsPackage.GLOBAL)
public class UpdateByColumn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SpecFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DeltaFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OptionsFieldType {
                    @JsOverlay
                    static UpdateByColumn.ToObjectReturnType.SpecFieldType.DeltaFieldType.OptionsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getNullBehavior();

                    @JsProperty
                    void setNullBehavior(double nullBehavior);
                }

                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.DeltaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByColumn.ToObjectReturnType.SpecFieldType.DeltaFieldType.OptionsFieldType getOptions();

                @JsProperty
                void setOptions(
                        UpdateByColumn.ToObjectReturnType.SpecFieldType.DeltaFieldType.OptionsFieldType options);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmMaxFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmMaxFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmMinFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmMinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmStdFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmStdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

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
                public interface WindowScaleFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TicksFieldType {
                        @JsOverlay
                        static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                        static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                    static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                    @JsProperty
                    void setTicks(
                            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                    @JsProperty
                    void setTime(
                            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
                }

                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                @JsProperty
                UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType getWindowScale();

                @JsProperty
                void setOptions(
                        UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                @JsProperty
                void setWindowScale(
                        UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType.WindowScaleFieldType windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmsFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.EmsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingAvgFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingCountFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingCountFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingGroupFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingGroupFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingMaxFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingMaxFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingMinFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingMinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingProductFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingProductFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingStdFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingStdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingSumFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingSumFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingWavgFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingWavgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static UpdateByColumn.ToObjectReturnType.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.DeltaFieldType getDelta();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmMaxFieldType getEmMax();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmMinFieldType getEmMin();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmStdFieldType getEmStd();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType getEma();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.EmsFieldType getEms();

            @JsProperty
            Object getFill();

            @JsProperty
            Object getMax();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getProduct();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingAvgFieldType getRollingAvg();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingCountFieldType getRollingCount();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingGroupFieldType getRollingGroup();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingMaxFieldType getRollingMax();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingMinFieldType getRollingMin();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingProductFieldType getRollingProduct();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingStdFieldType getRollingStd();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingSumFieldType getRollingSum();

            @JsProperty
            UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingWavgFieldType getRollingWavg();

            @JsProperty
            Object getSum();

            @JsProperty
            void setDelta(UpdateByColumn.ToObjectReturnType.SpecFieldType.DeltaFieldType delta);

            @JsProperty
            void setEmMax(UpdateByColumn.ToObjectReturnType.SpecFieldType.EmMaxFieldType emMax);

            @JsProperty
            void setEmMin(UpdateByColumn.ToObjectReturnType.SpecFieldType.EmMinFieldType emMin);

            @JsProperty
            void setEmStd(UpdateByColumn.ToObjectReturnType.SpecFieldType.EmStdFieldType emStd);

            @JsProperty
            void setEma(UpdateByColumn.ToObjectReturnType.SpecFieldType.EmaFieldType ema);

            @JsProperty
            void setEms(UpdateByColumn.ToObjectReturnType.SpecFieldType.EmsFieldType ems);

            @JsProperty
            void setFill(Object fill);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setProduct(Object product);

            @JsProperty
            void setRollingAvg(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingAvgFieldType rollingAvg);

            @JsProperty
            void setRollingCount(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingCountFieldType rollingCount);

            @JsProperty
            void setRollingGroup(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingGroupFieldType rollingGroup);

            @JsProperty
            void setRollingMax(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingMaxFieldType rollingMax);

            @JsProperty
            void setRollingMin(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingMinFieldType rollingMin);

            @JsProperty
            void setRollingProduct(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingProductFieldType rollingProduct);

            @JsProperty
            void setRollingStd(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingStdFieldType rollingStd);

            @JsProperty
            void setRollingSum(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingSumFieldType rollingSum);

            @JsProperty
            void setRollingWavg(
                    UpdateByColumn.ToObjectReturnType.SpecFieldType.RollingWavgFieldType rollingWavg);

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
            public interface DeltaFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OptionsFieldType {
                    @JsOverlay
                    static UpdateByColumn.ToObjectReturnType0.SpecFieldType.DeltaFieldType.OptionsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getNullBehavior();

                    @JsProperty
                    void setNullBehavior(double nullBehavior);
                }

                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.DeltaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByColumn.ToObjectReturnType0.SpecFieldType.DeltaFieldType.OptionsFieldType getOptions();

                @JsProperty
                void setOptions(
                        UpdateByColumn.ToObjectReturnType0.SpecFieldType.DeltaFieldType.OptionsFieldType options);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmMaxFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmMaxFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmMinFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmMinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmStdFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmStdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

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
                public interface WindowScaleFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TicksFieldType {
                        @JsOverlay
                        static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                        static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                    static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                    @JsProperty
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                    @JsProperty
                    void setTicks(
                            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                    @JsProperty
                    void setTime(
                            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
                }

                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                @JsProperty
                UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType getWindowScale();

                @JsProperty
                void setOptions(
                        UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.OptionsFieldType options);

                @JsProperty
                void setWindowScale(
                        UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType.WindowScaleFieldType windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmsFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getOptions();

                @JsProperty
                Object getWindowScale();

                @JsProperty
                void setOptions(Object options);

                @JsProperty
                void setWindowScale(Object windowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingAvgFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingCountFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingCountFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingGroupFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingGroupFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingMaxFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingMaxFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingMinFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingMinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingProductFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingProductFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingStdFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingStdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingSumFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingSumFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RollingWavgFieldType {
                @JsOverlay
                static UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingWavgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getForwardWindowScale();

                @JsProperty
                Object getReverseWindowScale();

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setForwardWindowScale(Object forwardWindowScale);

                @JsProperty
                void setReverseWindowScale(Object reverseWindowScale);

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static UpdateByColumn.ToObjectReturnType0.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.DeltaFieldType getDelta();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmMaxFieldType getEmMax();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmMinFieldType getEmMin();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmStdFieldType getEmStd();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType getEma();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmsFieldType getEms();

            @JsProperty
            Object getFill();

            @JsProperty
            Object getMax();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getProduct();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingAvgFieldType getRollingAvg();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingCountFieldType getRollingCount();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingGroupFieldType getRollingGroup();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingMaxFieldType getRollingMax();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingMinFieldType getRollingMin();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingProductFieldType getRollingProduct();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingStdFieldType getRollingStd();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingSumFieldType getRollingSum();

            @JsProperty
            UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingWavgFieldType getRollingWavg();

            @JsProperty
            Object getSum();

            @JsProperty
            void setDelta(UpdateByColumn.ToObjectReturnType0.SpecFieldType.DeltaFieldType delta);

            @JsProperty
            void setEmMax(UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmMaxFieldType emMax);

            @JsProperty
            void setEmMin(UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmMinFieldType emMin);

            @JsProperty
            void setEmStd(UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmStdFieldType emStd);

            @JsProperty
            void setEma(UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmaFieldType ema);

            @JsProperty
            void setEms(UpdateByColumn.ToObjectReturnType0.SpecFieldType.EmsFieldType ems);

            @JsProperty
            void setFill(Object fill);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setProduct(Object product);

            @JsProperty
            void setRollingAvg(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingAvgFieldType rollingAvg);

            @JsProperty
            void setRollingCount(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingCountFieldType rollingCount);

            @JsProperty
            void setRollingGroup(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingGroupFieldType rollingGroup);

            @JsProperty
            void setRollingMax(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingMaxFieldType rollingMax);

            @JsProperty
            void setRollingMin(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingMinFieldType rollingMin);

            @JsProperty
            void setRollingProduct(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingProductFieldType rollingProduct);

            @JsProperty
            void setRollingStd(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingStdFieldType rollingStd);

            @JsProperty
            void setRollingSum(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingSumFieldType rollingSum);

            @JsProperty
            void setRollingWavg(
                    UpdateByColumn.ToObjectReturnType0.SpecFieldType.RollingWavgFieldType rollingWavg);

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

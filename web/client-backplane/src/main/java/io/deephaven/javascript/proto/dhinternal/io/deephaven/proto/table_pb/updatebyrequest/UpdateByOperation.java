/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.UpdateByColumn;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation",
        namespace = JsPackage.GLOBAL)
public class UpdateByOperation {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SpecFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface DeltaFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface OptionsFieldType {
                        @JsOverlay
                        static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getNullBehavior();

                        @JsProperty
                        void setNullBehavior(double nullBehavior);
                    }

                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.DeltaFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType getOptions();

                    @JsProperty
                    void setOptions(
                            UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType options);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface EmMaxFieldType {
                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmMaxFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmMinFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmStdFieldType create() {
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
                    public interface WindowScaleFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface TicksFieldType {
                            @JsOverlay
                            static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                            static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                        static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                        @JsProperty
                        void setTicks(
                                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                        @JsProperty
                        void setTime(
                                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
                    }

                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType getWindowScale();

                    @JsProperty
                    void setOptions(
                            UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                    @JsProperty
                    void setWindowScale(
                            UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType windowScale);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface EmsFieldType {
                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmsFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingAvgFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingCountFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingGroupFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingMaxFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingMinFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingProductFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingStdFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingSumFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingWavgFieldType create() {
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
                static UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.DeltaFieldType getDelta();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmMaxFieldType getEmMax();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmMinFieldType getEmMin();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmStdFieldType getEmStd();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmsFieldType getEms();

                @JsProperty
                Object getFill();

                @JsProperty
                Object getMax();

                @JsProperty
                Object getMin();

                @JsProperty
                Object getProduct();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingAvgFieldType getRollingAvg();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingCountFieldType getRollingCount();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingGroupFieldType getRollingGroup();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingMaxFieldType getRollingMax();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingMinFieldType getRollingMin();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingProductFieldType getRollingProduct();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingStdFieldType getRollingStd();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingSumFieldType getRollingSum();

                @JsProperty
                UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingWavgFieldType getRollingWavg();

                @JsProperty
                Object getSum();

                @JsProperty
                void setDelta(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.DeltaFieldType delta);

                @JsProperty
                void setEmMax(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmMaxFieldType emMax);

                @JsProperty
                void setEmMin(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmMinFieldType emMin);

                @JsProperty
                void setEmStd(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmStdFieldType emStd);

                @JsProperty
                void setEma(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

                @JsProperty
                void setEms(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.EmsFieldType ems);

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
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingAvgFieldType rollingAvg);

                @JsProperty
                void setRollingCount(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingCountFieldType rollingCount);

                @JsProperty
                void setRollingGroup(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingGroupFieldType rollingGroup);

                @JsProperty
                void setRollingMax(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingMaxFieldType rollingMax);

                @JsProperty
                void setRollingMin(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingMinFieldType rollingMin);

                @JsProperty
                void setRollingProduct(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingProductFieldType rollingProduct);

                @JsProperty
                void setRollingStd(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingStdFieldType rollingStd);

                @JsProperty
                void setRollingSum(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingSumFieldType rollingSum);

                @JsProperty
                void setRollingWavg(
                        UpdateByOperation.ToObjectReturnType.ColumnFieldType.SpecFieldType.RollingWavgFieldType rollingWavg);

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
                public interface DeltaFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface OptionsFieldType {
                        @JsOverlay
                        static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getNullBehavior();

                        @JsProperty
                        void setNullBehavior(double nullBehavior);
                    }

                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.DeltaFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType getOptions();

                    @JsProperty
                    void setOptions(
                            UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType options);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface EmMaxFieldType {
                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmMaxFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmMinFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmStdFieldType create() {
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
                    public interface WindowScaleFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface TicksFieldType {
                            @JsOverlay
                            static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                            static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                        static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                        @JsProperty
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                        @JsProperty
                        void setTicks(
                                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                        @JsProperty
                        void setTime(
                                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
                    }

                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                    @JsProperty
                    UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType getWindowScale();

                    @JsProperty
                    void setOptions(
                            UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                    @JsProperty
                    void setWindowScale(
                            UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType windowScale);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface EmsFieldType {
                    @JsOverlay
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmsFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingAvgFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingCountFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingGroupFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingMaxFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingMinFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingProductFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingStdFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingSumFieldType create() {
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
                    static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingWavgFieldType create() {
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
                static UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.DeltaFieldType getDelta();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmMaxFieldType getEmMax();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmMinFieldType getEmMin();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmStdFieldType getEmStd();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmsFieldType getEms();

                @JsProperty
                Object getFill();

                @JsProperty
                Object getMax();

                @JsProperty
                Object getMin();

                @JsProperty
                Object getProduct();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingAvgFieldType getRollingAvg();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingCountFieldType getRollingCount();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingGroupFieldType getRollingGroup();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingMaxFieldType getRollingMax();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingMinFieldType getRollingMin();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingProductFieldType getRollingProduct();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingStdFieldType getRollingStd();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingSumFieldType getRollingSum();

                @JsProperty
                UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingWavgFieldType getRollingWavg();

                @JsProperty
                Object getSum();

                @JsProperty
                void setDelta(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.DeltaFieldType delta);

                @JsProperty
                void setEmMax(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmMaxFieldType emMax);

                @JsProperty
                void setEmMin(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmMinFieldType emMin);

                @JsProperty
                void setEmStd(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmStdFieldType emStd);

                @JsProperty
                void setEma(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmaFieldType ema);

                @JsProperty
                void setEms(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.EmsFieldType ems);

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
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingAvgFieldType rollingAvg);

                @JsProperty
                void setRollingCount(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingCountFieldType rollingCount);

                @JsProperty
                void setRollingGroup(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingGroupFieldType rollingGroup);

                @JsProperty
                void setRollingMax(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingMaxFieldType rollingMax);

                @JsProperty
                void setRollingMin(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingMinFieldType rollingMin);

                @JsProperty
                void setRollingProduct(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingProductFieldType rollingProduct);

                @JsProperty
                void setRollingStd(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingStdFieldType rollingStd);

                @JsProperty
                void setRollingSum(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingSumFieldType rollingSum);

                @JsProperty
                void setRollingWavg(
                        UpdateByOperation.ToObjectReturnType0.ColumnFieldType.SpecFieldType.RollingWavgFieldType rollingWavg);

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

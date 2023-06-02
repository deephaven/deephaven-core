/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeMax;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeMin;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeProduct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByCumulativeSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByDelta;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByEmMax;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByEmMin;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByEmStd;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByEma;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByEms;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByFill;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingAvg;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingCount;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingGroup;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingMax;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingMin;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingProduct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingStd;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.UpdateByRollingWAvg;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec",
        namespace = JsPackage.GLOBAL)
public class UpdateBySpec {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DeltaFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OptionsFieldType {
                @JsOverlay
                static UpdateBySpec.ToObjectReturnType.DeltaFieldType.OptionsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getNullBehavior();

                @JsProperty
                void setNullBehavior(double nullBehavior);
            }

            @JsOverlay
            static UpdateBySpec.ToObjectReturnType.DeltaFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateBySpec.ToObjectReturnType.DeltaFieldType.OptionsFieldType getOptions();

            @JsProperty
            void setOptions(UpdateBySpec.ToObjectReturnType.DeltaFieldType.OptionsFieldType options);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmMaxFieldType {
            @JsOverlay
            static UpdateBySpec.ToObjectReturnType.EmMaxFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.EmMinFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.EmStdFieldType create() {
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
            public interface WindowScaleFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicksFieldType {
                    @JsOverlay
                    static UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                    static UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                static UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                @JsProperty
                UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                @JsProperty
                void setTicks(
                        UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                @JsProperty
                void setTime(
                        UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
            }

            @JsOverlay
            static UpdateBySpec.ToObjectReturnType.EmaFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType getOptions();

            @JsProperty
            UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType getWindowScale();

            @JsProperty
            void setOptions(UpdateBySpec.ToObjectReturnType.EmaFieldType.OptionsFieldType options);

            @JsProperty
            void setWindowScale(
                    UpdateBySpec.ToObjectReturnType.EmaFieldType.WindowScaleFieldType windowScale);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmsFieldType {
            @JsOverlay
            static UpdateBySpec.ToObjectReturnType.EmsFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingAvgFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingCountFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingGroupFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingMaxFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingMinFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingProductFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingStdFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingSumFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType.RollingWavgFieldType create() {
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
        static UpdateBySpec.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateBySpec.ToObjectReturnType.DeltaFieldType getDelta();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.EmMaxFieldType getEmMax();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.EmMinFieldType getEmMin();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.EmStdFieldType getEmStd();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.EmaFieldType getEma();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.EmsFieldType getEms();

        @JsProperty
        Object getFill();

        @JsProperty
        Object getMax();

        @JsProperty
        Object getMin();

        @JsProperty
        Object getProduct();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingAvgFieldType getRollingAvg();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingCountFieldType getRollingCount();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingGroupFieldType getRollingGroup();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingMaxFieldType getRollingMax();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingMinFieldType getRollingMin();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingProductFieldType getRollingProduct();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingStdFieldType getRollingStd();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingSumFieldType getRollingSum();

        @JsProperty
        UpdateBySpec.ToObjectReturnType.RollingWavgFieldType getRollingWavg();

        @JsProperty
        Object getSum();

        @JsProperty
        void setDelta(UpdateBySpec.ToObjectReturnType.DeltaFieldType delta);

        @JsProperty
        void setEmMax(UpdateBySpec.ToObjectReturnType.EmMaxFieldType emMax);

        @JsProperty
        void setEmMin(UpdateBySpec.ToObjectReturnType.EmMinFieldType emMin);

        @JsProperty
        void setEmStd(UpdateBySpec.ToObjectReturnType.EmStdFieldType emStd);

        @JsProperty
        void setEma(UpdateBySpec.ToObjectReturnType.EmaFieldType ema);

        @JsProperty
        void setEms(UpdateBySpec.ToObjectReturnType.EmsFieldType ems);

        @JsProperty
        void setFill(Object fill);

        @JsProperty
        void setMax(Object max);

        @JsProperty
        void setMin(Object min);

        @JsProperty
        void setProduct(Object product);

        @JsProperty
        void setRollingAvg(UpdateBySpec.ToObjectReturnType.RollingAvgFieldType rollingAvg);

        @JsProperty
        void setRollingCount(UpdateBySpec.ToObjectReturnType.RollingCountFieldType rollingCount);

        @JsProperty
        void setRollingGroup(UpdateBySpec.ToObjectReturnType.RollingGroupFieldType rollingGroup);

        @JsProperty
        void setRollingMax(UpdateBySpec.ToObjectReturnType.RollingMaxFieldType rollingMax);

        @JsProperty
        void setRollingMin(UpdateBySpec.ToObjectReturnType.RollingMinFieldType rollingMin);

        @JsProperty
        void setRollingProduct(UpdateBySpec.ToObjectReturnType.RollingProductFieldType rollingProduct);

        @JsProperty
        void setRollingStd(UpdateBySpec.ToObjectReturnType.RollingStdFieldType rollingStd);

        @JsProperty
        void setRollingSum(UpdateBySpec.ToObjectReturnType.RollingSumFieldType rollingSum);

        @JsProperty
        void setRollingWavg(UpdateBySpec.ToObjectReturnType.RollingWavgFieldType rollingWavg);

        @JsProperty
        void setSum(Object sum);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DeltaFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OptionsFieldType {
                @JsOverlay
                static UpdateBySpec.ToObjectReturnType0.DeltaFieldType.OptionsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getNullBehavior();

                @JsProperty
                void setNullBehavior(double nullBehavior);
            }

            @JsOverlay
            static UpdateBySpec.ToObjectReturnType0.DeltaFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateBySpec.ToObjectReturnType0.DeltaFieldType.OptionsFieldType getOptions();

            @JsProperty
            void setOptions(UpdateBySpec.ToObjectReturnType0.DeltaFieldType.OptionsFieldType options);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmMaxFieldType {
            @JsOverlay
            static UpdateBySpec.ToObjectReturnType0.EmMaxFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.EmMinFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.EmStdFieldType create() {
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
            public interface WindowScaleFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicksFieldType {
                    @JsOverlay
                    static UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                    static UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                static UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                @JsProperty
                UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                @JsProperty
                void setTicks(
                        UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                @JsProperty
                void setTime(
                        UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
            }

            @JsOverlay
            static UpdateBySpec.ToObjectReturnType0.EmaFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType getOptions();

            @JsProperty
            UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType getWindowScale();

            @JsProperty
            void setOptions(UpdateBySpec.ToObjectReturnType0.EmaFieldType.OptionsFieldType options);

            @JsProperty
            void setWindowScale(
                    UpdateBySpec.ToObjectReturnType0.EmaFieldType.WindowScaleFieldType windowScale);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmsFieldType {
            @JsOverlay
            static UpdateBySpec.ToObjectReturnType0.EmsFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingAvgFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingCountFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingGroupFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingMaxFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingMinFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingProductFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingStdFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingSumFieldType create() {
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
            static UpdateBySpec.ToObjectReturnType0.RollingWavgFieldType create() {
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
        static UpdateBySpec.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.DeltaFieldType getDelta();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.EmMaxFieldType getEmMax();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.EmMinFieldType getEmMin();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.EmStdFieldType getEmStd();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.EmaFieldType getEma();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.EmsFieldType getEms();

        @JsProperty
        Object getFill();

        @JsProperty
        Object getMax();

        @JsProperty
        Object getMin();

        @JsProperty
        Object getProduct();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingAvgFieldType getRollingAvg();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingCountFieldType getRollingCount();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingGroupFieldType getRollingGroup();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingMaxFieldType getRollingMax();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingMinFieldType getRollingMin();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingProductFieldType getRollingProduct();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingStdFieldType getRollingStd();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingSumFieldType getRollingSum();

        @JsProperty
        UpdateBySpec.ToObjectReturnType0.RollingWavgFieldType getRollingWavg();

        @JsProperty
        Object getSum();

        @JsProperty
        void setDelta(UpdateBySpec.ToObjectReturnType0.DeltaFieldType delta);

        @JsProperty
        void setEmMax(UpdateBySpec.ToObjectReturnType0.EmMaxFieldType emMax);

        @JsProperty
        void setEmMin(UpdateBySpec.ToObjectReturnType0.EmMinFieldType emMin);

        @JsProperty
        void setEmStd(UpdateBySpec.ToObjectReturnType0.EmStdFieldType emStd);

        @JsProperty
        void setEma(UpdateBySpec.ToObjectReturnType0.EmaFieldType ema);

        @JsProperty
        void setEms(UpdateBySpec.ToObjectReturnType0.EmsFieldType ems);

        @JsProperty
        void setFill(Object fill);

        @JsProperty
        void setMax(Object max);

        @JsProperty
        void setMin(Object min);

        @JsProperty
        void setProduct(Object product);

        @JsProperty
        void setRollingAvg(UpdateBySpec.ToObjectReturnType0.RollingAvgFieldType rollingAvg);

        @JsProperty
        void setRollingCount(UpdateBySpec.ToObjectReturnType0.RollingCountFieldType rollingCount);

        @JsProperty
        void setRollingGroup(UpdateBySpec.ToObjectReturnType0.RollingGroupFieldType rollingGroup);

        @JsProperty
        void setRollingMax(UpdateBySpec.ToObjectReturnType0.RollingMaxFieldType rollingMax);

        @JsProperty
        void setRollingMin(UpdateBySpec.ToObjectReturnType0.RollingMinFieldType rollingMin);

        @JsProperty
        void setRollingProduct(UpdateBySpec.ToObjectReturnType0.RollingProductFieldType rollingProduct);

        @JsProperty
        void setRollingStd(UpdateBySpec.ToObjectReturnType0.RollingStdFieldType rollingStd);

        @JsProperty
        void setRollingSum(UpdateBySpec.ToObjectReturnType0.RollingSumFieldType rollingSum);

        @JsProperty
        void setRollingWavg(UpdateBySpec.ToObjectReturnType0.RollingWavgFieldType rollingWavg);

        @JsProperty
        void setSum(Object sum);
    }

    public static native UpdateBySpec deserializeBinary(Uint8Array bytes);

    public static native UpdateBySpec deserializeBinaryFromReader(
            UpdateBySpec message, Object reader);

    public static native void serializeBinaryToWriter(UpdateBySpec message, Object writer);

    public static native UpdateBySpec.ToObjectReturnType toObject(
            boolean includeInstance, UpdateBySpec msg);

    public native void clearDelta();

    public native void clearEmMax();

    public native void clearEmMin();

    public native void clearEmStd();

    public native void clearEma();

    public native void clearEms();

    public native void clearFill();

    public native void clearMax();

    public native void clearMin();

    public native void clearProduct();

    public native void clearRollingAvg();

    public native void clearRollingCount();

    public native void clearRollingGroup();

    public native void clearRollingMax();

    public native void clearRollingMin();

    public native void clearRollingProduct();

    public native void clearRollingStd();

    public native void clearRollingSum();

    public native void clearRollingWavg();

    public native void clearSum();

    public native UpdateByDelta getDelta();

    public native UpdateByEmMax getEmMax();

    public native UpdateByEmMin getEmMin();

    public native UpdateByEmStd getEmStd();

    public native UpdateByEma getEma();

    public native UpdateByEms getEms();

    public native UpdateByFill getFill();

    public native UpdateByCumulativeMax getMax();

    public native UpdateByCumulativeMin getMin();

    public native UpdateByCumulativeProduct getProduct();

    public native UpdateByRollingAvg getRollingAvg();

    public native UpdateByRollingCount getRollingCount();

    public native UpdateByRollingGroup getRollingGroup();

    public native UpdateByRollingMax getRollingMax();

    public native UpdateByRollingMin getRollingMin();

    public native UpdateByRollingProduct getRollingProduct();

    public native UpdateByRollingStd getRollingStd();

    public native UpdateByRollingSum getRollingSum();

    public native UpdateByRollingWAvg getRollingWavg();

    public native UpdateByCumulativeSum getSum();

    public native int getTypeCase();

    public native boolean hasDelta();

    public native boolean hasEmMax();

    public native boolean hasEmMin();

    public native boolean hasEmStd();

    public native boolean hasEma();

    public native boolean hasEms();

    public native boolean hasFill();

    public native boolean hasMax();

    public native boolean hasMin();

    public native boolean hasProduct();

    public native boolean hasRollingAvg();

    public native boolean hasRollingCount();

    public native boolean hasRollingGroup();

    public native boolean hasRollingMax();

    public native boolean hasRollingMin();

    public native boolean hasRollingProduct();

    public native boolean hasRollingStd();

    public native boolean hasRollingSum();

    public native boolean hasRollingWavg();

    public native boolean hasSum();

    public native Uint8Array serializeBinary();

    public native void setDelta();

    public native void setDelta(UpdateByDelta value);

    public native void setEmMax();

    public native void setEmMax(UpdateByEmMax value);

    public native void setEmMin();

    public native void setEmMin(UpdateByEmMin value);

    public native void setEmStd();

    public native void setEmStd(UpdateByEmStd value);

    public native void setEma();

    public native void setEma(UpdateByEma value);

    public native void setEms();

    public native void setEms(UpdateByEms value);

    public native void setFill();

    public native void setFill(UpdateByFill value);

    public native void setMax();

    public native void setMax(UpdateByCumulativeMax value);

    public native void setMin();

    public native void setMin(UpdateByCumulativeMin value);

    public native void setProduct();

    public native void setProduct(UpdateByCumulativeProduct value);

    public native void setRollingAvg();

    public native void setRollingAvg(UpdateByRollingAvg value);

    public native void setRollingCount();

    public native void setRollingCount(UpdateByRollingCount value);

    public native void setRollingGroup();

    public native void setRollingGroup(UpdateByRollingGroup value);

    public native void setRollingMax();

    public native void setRollingMax(UpdateByRollingMax value);

    public native void setRollingMin();

    public native void setRollingMin(UpdateByRollingMin value);

    public native void setRollingProduct();

    public native void setRollingProduct(UpdateByRollingProduct value);

    public native void setRollingStd();

    public native void setRollingStd(UpdateByRollingStd value);

    public native void setRollingSum();

    public native void setRollingSum(UpdateByRollingSum value);

    public native void setRollingWavg();

    public native void setRollingWavg(UpdateByRollingWAvg value);

    public native void setSum();

    public native void setSum(UpdateByCumulativeSum value);

    public native UpdateBySpec.ToObjectReturnType0 toObject();

    public native UpdateBySpec.ToObjectReturnType0 toObject(boolean includeInstance);
}

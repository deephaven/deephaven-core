/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.UpdateByOperation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.UpdateByOptions;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest",
        namespace = JsPackage.GLOBAL)
public class UpdateByRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OperationsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ColumnFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SpecFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface DeltaFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface OptionsFieldType {
                            @JsOverlay
                            static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            double getNullBehavior();

                            @JsProperty
                            void setNullBehavior(double nullBehavior);
                        }

                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType getOptions();

                        @JsProperty
                        void setOptions(
                                UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType options);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface EmMaxFieldType {
                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMaxFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMinFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmStdFieldType create() {
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
                            @JsOverlay
                            static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            Object getBigValueContext();

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
                            void setBigValueContext(Object bigValueContext);

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
                                static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                                static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                            static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                            @JsProperty
                            void setTicks(
                                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                            @JsProperty
                            void setTime(
                                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
                        }

                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType getWindowScale();

                        @JsProperty
                        void setOptions(
                                UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                        @JsProperty
                        void setWindowScale(
                                UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType windowScale);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface EmsFieldType {
                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmsFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingAvgFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingCountFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingGroupFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMaxFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMinFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingProductFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingStdFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingSumFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingWavgFieldType create() {
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
                    static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType getDelta();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMaxFieldType getEmMax();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMinFieldType getEmMin();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmStdFieldType getEmStd();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmsFieldType getEms();

                    @JsProperty
                    Object getFill();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getProduct();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingAvgFieldType getRollingAvg();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingCountFieldType getRollingCount();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingGroupFieldType getRollingGroup();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMaxFieldType getRollingMax();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMinFieldType getRollingMin();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingProductFieldType getRollingProduct();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingStdFieldType getRollingStd();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingSumFieldType getRollingSum();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingWavgFieldType getRollingWavg();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    void setDelta(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType delta);

                    @JsProperty
                    void setEmMax(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMaxFieldType emMax);

                    @JsProperty
                    void setEmMin(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMinFieldType emMin);

                    @JsProperty
                    void setEmStd(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmStdFieldType emStd);

                    @JsProperty
                    void setEma(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

                    @JsProperty
                    void setEms(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmsFieldType ems);

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
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingAvgFieldType rollingAvg);

                    @JsProperty
                    void setRollingCount(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingCountFieldType rollingCount);

                    @JsProperty
                    void setRollingGroup(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingGroupFieldType rollingGroup);

                    @JsProperty
                    void setRollingMax(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMaxFieldType rollingMax);

                    @JsProperty
                    void setRollingMin(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMinFieldType rollingMin);

                    @JsProperty
                    void setRollingProduct(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingProductFieldType rollingProduct);

                    @JsProperty
                    void setRollingStd(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingStdFieldType rollingStd);

                    @JsProperty
                    void setRollingSum(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingSumFieldType rollingSum);

                    @JsProperty
                    void setRollingWavg(
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingWavgFieldType rollingWavg);

                    @JsProperty
                    void setSum(Object sum);
                }

                @JsOverlay
                static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType spec);
            }

            @JsOverlay
            static UpdateByRequest.ToObjectReturnType.OperationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType getColumn();

            @JsProperty
            void setColumn(
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType column);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface MathContextFieldType {
                @JsOverlay
                static UpdateByRequest.ToObjectReturnType.OptionsFieldType.MathContextFieldType create() {
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
            static UpdateByRequest.ToObjectReturnType.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getChunkCapacity();

            @JsProperty
            double getInitialHashTableSize();

            @JsProperty
            UpdateByRequest.ToObjectReturnType.OptionsFieldType.MathContextFieldType getMathContext();

            @JsProperty
            double getMaxStaticSparseMemoryOverhead();

            @JsProperty
            double getMaximumLoadFactor();

            @JsProperty
            double getTargetLoadFactor();

            @JsProperty
            boolean isUseRedirection();

            @JsProperty
            void setChunkCapacity(double chunkCapacity);

            @JsProperty
            void setInitialHashTableSize(double initialHashTableSize);

            @JsProperty
            void setMathContext(
                    UpdateByRequest.ToObjectReturnType.OptionsFieldType.MathContextFieldType mathContext);

            @JsProperty
            void setMaxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

            @JsProperty
            void setMaximumLoadFactor(double maximumLoadFactor);

            @JsProperty
            void setTargetLoadFactor(double targetLoadFactor);

            @JsProperty
            void setUseRedirection(boolean useRedirection);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static UpdateByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
                        Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static UpdateByRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    UpdateByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<UpdateByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<UpdateByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static UpdateByRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsOverlay
        static UpdateByRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        JsArray<UpdateByRequest.ToObjectReturnType.OperationsListFieldType> getOperationsList();

        @JsProperty
        UpdateByRequest.ToObjectReturnType.OptionsFieldType getOptions();

        @JsProperty
        UpdateByRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        UpdateByRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setOperationsList(
                JsArray<UpdateByRequest.ToObjectReturnType.OperationsListFieldType> operationsList);

        @JsOverlay
        default void setOperationsList(
                UpdateByRequest.ToObjectReturnType.OperationsListFieldType[] operationsList) {
            setOperationsList(
                    Js.<JsArray<UpdateByRequest.ToObjectReturnType.OperationsListFieldType>>uncheckedCast(
                            operationsList));
        }

        @JsProperty
        void setOptions(UpdateByRequest.ToObjectReturnType.OptionsFieldType options);

        @JsProperty
        void setResultId(UpdateByRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(UpdateByRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OperationsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ColumnFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SpecFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface DeltaFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface OptionsFieldType {
                            @JsOverlay
                            static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            double getNullBehavior();

                            @JsProperty
                            void setNullBehavior(double nullBehavior);
                        }

                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType getOptions();

                        @JsProperty
                        void setOptions(
                                UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType.OptionsFieldType options);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface EmMaxFieldType {
                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMaxFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMinFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmStdFieldType create() {
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
                            @JsOverlay
                            static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            Object getBigValueContext();

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
                            void setBigValueContext(Object bigValueContext);

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
                                static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType create() {
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
                                static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType create() {
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
                            static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType getTicks();

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType getTime();

                            @JsProperty
                            void setTicks(
                                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TicksFieldType ticks);

                            @JsProperty
                            void setTime(
                                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType.TimeFieldType time);
                        }

                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType getWindowScale();

                        @JsProperty
                        void setOptions(
                                UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                        @JsProperty
                        void setWindowScale(
                                UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.WindowScaleFieldType windowScale);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface EmsFieldType {
                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmsFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingAvgFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingCountFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingGroupFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMaxFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMinFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingProductFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingStdFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingSumFieldType create() {
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
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingWavgFieldType create() {
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
                    static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType getDelta();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMaxFieldType getEmMax();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMinFieldType getEmMin();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmStdFieldType getEmStd();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmsFieldType getEms();

                    @JsProperty
                    Object getFill();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getProduct();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingAvgFieldType getRollingAvg();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingCountFieldType getRollingCount();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingGroupFieldType getRollingGroup();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMaxFieldType getRollingMax();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMinFieldType getRollingMin();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingProductFieldType getRollingProduct();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingStdFieldType getRollingStd();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingSumFieldType getRollingSum();

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingWavgFieldType getRollingWavg();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    void setDelta(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.DeltaFieldType delta);

                    @JsProperty
                    void setEmMax(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMaxFieldType emMax);

                    @JsProperty
                    void setEmMin(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmMinFieldType emMin);

                    @JsProperty
                    void setEmStd(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmStdFieldType emStd);

                    @JsProperty
                    void setEma(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

                    @JsProperty
                    void setEms(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmsFieldType ems);

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
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingAvgFieldType rollingAvg);

                    @JsProperty
                    void setRollingCount(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingCountFieldType rollingCount);

                    @JsProperty
                    void setRollingGroup(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingGroupFieldType rollingGroup);

                    @JsProperty
                    void setRollingMax(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMaxFieldType rollingMax);

                    @JsProperty
                    void setRollingMin(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingMinFieldType rollingMin);

                    @JsProperty
                    void setRollingProduct(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingProductFieldType rollingProduct);

                    @JsProperty
                    void setRollingStd(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingStdFieldType rollingStd);

                    @JsProperty
                    void setRollingSum(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingSumFieldType rollingSum);

                    @JsProperty
                    void setRollingWavg(
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.RollingWavgFieldType rollingWavg);

                    @JsProperty
                    void setSum(Object sum);
                }

                @JsOverlay
                static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType spec);
            }

            @JsOverlay
            static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType getColumn();

            @JsProperty
            void setColumn(
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType column);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface MathContextFieldType {
                @JsOverlay
                static UpdateByRequest.ToObjectReturnType0.OptionsFieldType.MathContextFieldType create() {
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
            static UpdateByRequest.ToObjectReturnType0.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getChunkCapacity();

            @JsProperty
            double getInitialHashTableSize();

            @JsProperty
            UpdateByRequest.ToObjectReturnType0.OptionsFieldType.MathContextFieldType getMathContext();

            @JsProperty
            double getMaxStaticSparseMemoryOverhead();

            @JsProperty
            double getMaximumLoadFactor();

            @JsProperty
            double getTargetLoadFactor();

            @JsProperty
            boolean isUseRedirection();

            @JsProperty
            void setChunkCapacity(double chunkCapacity);

            @JsProperty
            void setInitialHashTableSize(double initialHashTableSize);

            @JsProperty
            void setMathContext(
                    UpdateByRequest.ToObjectReturnType0.OptionsFieldType.MathContextFieldType mathContext);

            @JsProperty
            void setMaxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

            @JsProperty
            void setMaximumLoadFactor(double maximumLoadFactor);

            @JsProperty
            void setTargetLoadFactor(double targetLoadFactor);

            @JsProperty
            void setUseRedirection(boolean useRedirection);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static UpdateByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
                        Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static UpdateByRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    UpdateByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<UpdateByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<UpdateByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static UpdateByRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsOverlay
        static UpdateByRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        JsArray<UpdateByRequest.ToObjectReturnType0.OperationsListFieldType> getOperationsList();

        @JsProperty
        UpdateByRequest.ToObjectReturnType0.OptionsFieldType getOptions();

        @JsProperty
        UpdateByRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        UpdateByRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setOperationsList(
                JsArray<UpdateByRequest.ToObjectReturnType0.OperationsListFieldType> operationsList);

        @JsOverlay
        default void setOperationsList(
                UpdateByRequest.ToObjectReturnType0.OperationsListFieldType[] operationsList) {
            setOperationsList(
                    Js.<JsArray<UpdateByRequest.ToObjectReturnType0.OperationsListFieldType>>uncheckedCast(
                            operationsList));
        }

        @JsProperty
        void setOptions(UpdateByRequest.ToObjectReturnType0.OptionsFieldType options);

        @JsProperty
        void setResultId(UpdateByRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(UpdateByRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native UpdateByRequest deserializeBinary(Uint8Array bytes);

    public static native UpdateByRequest deserializeBinaryFromReader(
            UpdateByRequest message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByRequest message, Object writer);

    public static native UpdateByRequest.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByRequest msg);

    public native String addGroupByColumns(String value, double index);

    public native String addGroupByColumns(String value);

    public native UpdateByOperation addOperations();

    public native UpdateByOperation addOperations(UpdateByOperation value, double index);

    public native UpdateByOperation addOperations(UpdateByOperation value);

    public native void clearGroupByColumnsList();

    public native void clearOperationsList();

    public native void clearOptions();

    public native void clearResultId();

    public native void clearSourceId();

    public native JsArray<String> getGroupByColumnsList();

    public native JsArray<UpdateByOperation> getOperationsList();

    public native UpdateByOptions getOptions();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasOptions();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setGroupByColumnsList(JsArray<String> value);

    @JsOverlay
    public final void setGroupByColumnsList(String[] value) {
        setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setOperationsList(JsArray<UpdateByOperation> value);

    @JsOverlay
    public final void setOperationsList(UpdateByOperation[] value) {
        setOperationsList(Js.<JsArray<UpdateByOperation>>uncheckedCast(value));
    }

    public native void setOptions();

    public native void setOptions(UpdateByOptions value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native UpdateByRequest.ToObjectReturnType0 toObject();

    public native UpdateByRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

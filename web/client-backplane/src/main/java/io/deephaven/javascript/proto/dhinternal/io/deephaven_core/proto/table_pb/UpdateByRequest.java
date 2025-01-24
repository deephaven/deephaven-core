//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.UpdateByOperation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.UpdateByOptions;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest",
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
                        public interface TimescaleFieldType {
                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface TicksFieldType {
                                @JsOverlay
                                static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                                static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                            static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                            @JsProperty
                            void setTicks(
                                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                            @JsProperty
                            void setTime(
                                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                        }

                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                        @JsProperty
                        void setOptions(
                                UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                        @JsProperty
                        void setTimescale(
                                UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                    }

                    @JsOverlay
                    static UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

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
                            UpdateByRequest.ToObjectReturnType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

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
                        public interface TimescaleFieldType {
                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface TicksFieldType {
                                @JsOverlay
                                static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                                static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                            static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                            @JsProperty
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                            @JsProperty
                            void setTicks(
                                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                            @JsProperty
                            void setTime(
                                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                        }

                        @JsOverlay
                        static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                        @JsProperty
                        UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                        @JsProperty
                        void setOptions(
                                UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                        @JsProperty
                        void setTimescale(
                                UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                    }

                    @JsOverlay
                    static UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

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
                            UpdateByRequest.ToObjectReturnType0.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

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

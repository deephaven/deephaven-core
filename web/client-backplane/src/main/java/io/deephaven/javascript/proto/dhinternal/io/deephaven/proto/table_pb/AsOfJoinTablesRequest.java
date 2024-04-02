//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.asofjointablesrequest.MatchRuleMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AsOfJoinTablesRequest",
        namespace = JsPackage.GLOBAL)
public class AsOfJoinTablesRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static AsOfJoinTablesRequest.ToObjectReturnType.LeftIdFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static AsOfJoinTablesRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getAsOfMatchRule();

        @JsProperty
        JsArray<String> getColumnsToAddList();

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        AsOfJoinTablesRequest.ToObjectReturnType.LeftIdFieldType getLeftId();

        @JsProperty
        AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        void setAsOfMatchRule(double asOfMatchRule);

        @JsProperty
        void setColumnsToAddList(JsArray<String> columnsToAddList);

        @JsOverlay
        default void setColumnsToAddList(String[] columnsToAddList) {
            setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
        }

        @JsProperty
        void setColumnsToMatchList(JsArray<String> columnsToMatchList);

        @JsOverlay
        default void setColumnsToMatchList(String[] columnsToMatchList) {
            setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
        }

        @JsProperty
        void setLeftId(AsOfJoinTablesRequest.ToObjectReturnType.LeftIdFieldType leftId);

        @JsProperty
        void setResultId(AsOfJoinTablesRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static AsOfJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static AsOfJoinTablesRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getAsOfMatchRule();

        @JsProperty
        JsArray<String> getColumnsToAddList();

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        AsOfJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType getLeftId();

        @JsProperty
        AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        void setAsOfMatchRule(double asOfMatchRule);

        @JsProperty
        void setColumnsToAddList(JsArray<String> columnsToAddList);

        @JsOverlay
        default void setColumnsToAddList(String[] columnsToAddList) {
            setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
        }

        @JsProperty
        void setColumnsToMatchList(JsArray<String> columnsToMatchList);

        @JsOverlay
        default void setColumnsToMatchList(String[] columnsToMatchList) {
            setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
        }

        @JsProperty
        void setLeftId(AsOfJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType leftId);

        @JsProperty
        void setResultId(AsOfJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);
    }

    public static MatchRuleMap MatchRule;

    public static native AsOfJoinTablesRequest deserializeBinary(Uint8Array bytes);

    public static native AsOfJoinTablesRequest deserializeBinaryFromReader(
            AsOfJoinTablesRequest message, Object reader);

    public static native void serializeBinaryToWriter(AsOfJoinTablesRequest message, Object writer);

    public static native AsOfJoinTablesRequest.ToObjectReturnType toObject(
            boolean includeInstance, AsOfJoinTablesRequest msg);

    public native String addColumnsToAdd(String value, double index);

    public native String addColumnsToAdd(String value);

    public native String addColumnsToMatch(String value, double index);

    public native String addColumnsToMatch(String value);

    public native void clearColumnsToAddList();

    public native void clearColumnsToMatchList();

    public native void clearLeftId();

    public native void clearResultId();

    public native void clearRightId();

    public native double getAsOfMatchRule();

    public native JsArray<String> getColumnsToAddList();

    public native JsArray<String> getColumnsToMatchList();

    public native TableReference getLeftId();

    public native Ticket getResultId();

    public native TableReference getRightId();

    public native boolean hasLeftId();

    public native boolean hasResultId();

    public native boolean hasRightId();

    public native Uint8Array serializeBinary();

    public native void setAsOfMatchRule(double value);

    public native void setColumnsToAddList(JsArray<String> value);

    @JsOverlay
    public final void setColumnsToAddList(String[] value) {
        setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setColumnsToMatchList(JsArray<String> value);

    @JsOverlay
    public final void setColumnsToMatchList(String[] value) {
        setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setLeftId();

    public native void setLeftId(TableReference value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setRightId();

    public native void setRightId(TableReference value);

    public native AsOfJoinTablesRequest.ToObjectReturnType0 toObject();

    public native AsOfJoinTablesRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

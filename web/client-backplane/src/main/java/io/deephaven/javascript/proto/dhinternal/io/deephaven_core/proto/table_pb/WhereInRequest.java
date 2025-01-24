//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.WhereInRequest",
        namespace = JsPackage.GLOBAL)
public class WhereInRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static WhereInRequest.ToObjectReturnType.LeftIdFieldType create() {
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
                static WhereInRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(Object o) {
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
            static WhereInRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            WhereInRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(WhereInRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<WhereInRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<WhereInRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static WhereInRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        WhereInRequest.ToObjectReturnType.LeftIdFieldType getLeftId();

        @JsProperty
        WhereInRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        boolean isInverted();

        @JsProperty
        void setColumnsToMatchList(JsArray<String> columnsToMatchList);

        @JsOverlay
        default void setColumnsToMatchList(String[] columnsToMatchList) {
            setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
        }

        @JsProperty
        void setInverted(boolean inverted);

        @JsProperty
        void setLeftId(WhereInRequest.ToObjectReturnType.LeftIdFieldType leftId);

        @JsProperty
        void setResultId(WhereInRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static WhereInRequest.ToObjectReturnType0.LeftIdFieldType create() {
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
                static WhereInRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static WhereInRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            WhereInRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    WhereInRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<WhereInRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<WhereInRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static WhereInRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        WhereInRequest.ToObjectReturnType0.LeftIdFieldType getLeftId();

        @JsProperty
        WhereInRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        boolean isInverted();

        @JsProperty
        void setColumnsToMatchList(JsArray<String> columnsToMatchList);

        @JsOverlay
        default void setColumnsToMatchList(String[] columnsToMatchList) {
            setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
        }

        @JsProperty
        void setInverted(boolean inverted);

        @JsProperty
        void setLeftId(WhereInRequest.ToObjectReturnType0.LeftIdFieldType leftId);

        @JsProperty
        void setResultId(WhereInRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);
    }

    public static native WhereInRequest deserializeBinary(Uint8Array bytes);

    public static native WhereInRequest deserializeBinaryFromReader(
            WhereInRequest message, Object reader);

    public static native void serializeBinaryToWriter(WhereInRequest message, Object writer);

    public static native WhereInRequest.ToObjectReturnType toObject(
            boolean includeInstance, WhereInRequest msg);

    public native String addColumnsToMatch(String value, double index);

    public native String addColumnsToMatch(String value);

    public native void clearColumnsToMatchList();

    public native void clearLeftId();

    public native void clearResultId();

    public native void clearRightId();

    public native JsArray<String> getColumnsToMatchList();

    public native boolean getInverted();

    public native TableReference getLeftId();

    public native Ticket getResultId();

    public native TableReference getRightId();

    public native boolean hasLeftId();

    public native boolean hasResultId();

    public native boolean hasRightId();

    public native Uint8Array serializeBinary();

    public native void setColumnsToMatchList(JsArray<String> value);

    @JsOverlay
    public final void setColumnsToMatchList(String[] value) {
        setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setInverted(boolean value);

    public native void setLeftId();

    public native void setLeftId(TableReference value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setRightId();

    public native void setRightId(TableReference value);

    public native WhereInRequest.ToObjectReturnType0 toObject();

    public native WhereInRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

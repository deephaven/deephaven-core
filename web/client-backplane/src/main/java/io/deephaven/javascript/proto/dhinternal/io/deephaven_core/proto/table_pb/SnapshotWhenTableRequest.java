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
        name = "dhinternal.io.deephaven_core.proto.table_pb.SnapshotWhenTableRequest",
        namespace = JsPackage.GLOBAL)
public class SnapshotWhenTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BaseIdFieldType {
            @JsOverlay
            static SnapshotWhenTableRequest.ToObjectReturnType.BaseIdFieldType create() {
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
                static SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static SnapshotWhenTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        SnapshotWhenTableRequest.ToObjectReturnType.BaseIdFieldType getBaseId();

        @JsProperty
        SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        JsArray<String> getStampColumnsList();

        @JsProperty
        Object getTriggerId();

        @JsProperty
        boolean isHistory();

        @JsProperty
        boolean isIncremental();

        @JsProperty
        boolean isInitial();

        @JsProperty
        void setBaseId(SnapshotWhenTableRequest.ToObjectReturnType.BaseIdFieldType baseId);

        @JsProperty
        void setHistory(boolean history);

        @JsProperty
        void setIncremental(boolean incremental);

        @JsProperty
        void setInitial(boolean initial);

        @JsProperty
        void setResultId(SnapshotWhenTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setStampColumnsList(JsArray<String> stampColumnsList);

        @JsOverlay
        default void setStampColumnsList(String[] stampColumnsList) {
            setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
        }

        @JsProperty
        void setTriggerId(Object triggerId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BaseIdFieldType {
            @JsOverlay
            static SnapshotWhenTableRequest.ToObjectReturnType0.BaseIdFieldType create() {
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
                static SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static SnapshotWhenTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        SnapshotWhenTableRequest.ToObjectReturnType0.BaseIdFieldType getBaseId();

        @JsProperty
        SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        JsArray<String> getStampColumnsList();

        @JsProperty
        Object getTriggerId();

        @JsProperty
        boolean isHistory();

        @JsProperty
        boolean isIncremental();

        @JsProperty
        boolean isInitial();

        @JsProperty
        void setBaseId(SnapshotWhenTableRequest.ToObjectReturnType0.BaseIdFieldType baseId);

        @JsProperty
        void setHistory(boolean history);

        @JsProperty
        void setIncremental(boolean incremental);

        @JsProperty
        void setInitial(boolean initial);

        @JsProperty
        void setResultId(SnapshotWhenTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setStampColumnsList(JsArray<String> stampColumnsList);

        @JsOverlay
        default void setStampColumnsList(String[] stampColumnsList) {
            setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
        }

        @JsProperty
        void setTriggerId(Object triggerId);
    }

    public static native SnapshotWhenTableRequest deserializeBinary(Uint8Array bytes);

    public static native SnapshotWhenTableRequest deserializeBinaryFromReader(
            SnapshotWhenTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            SnapshotWhenTableRequest message, Object writer);

    public static native SnapshotWhenTableRequest.ToObjectReturnType toObject(
            boolean includeInstance, SnapshotWhenTableRequest msg);

    public native String addStampColumns(String value, double index);

    public native String addStampColumns(String value);

    public native void clearBaseId();

    public native void clearResultId();

    public native void clearStampColumnsList();

    public native void clearTriggerId();

    public native TableReference getBaseId();

    public native boolean getHistory();

    public native boolean getIncremental();

    public native boolean getInitial();

    public native Ticket getResultId();

    public native JsArray<String> getStampColumnsList();

    public native TableReference getTriggerId();

    public native boolean hasBaseId();

    public native boolean hasResultId();

    public native boolean hasTriggerId();

    public native Uint8Array serializeBinary();

    public native void setBaseId();

    public native void setBaseId(TableReference value);

    public native void setHistory(boolean value);

    public native void setIncremental(boolean value);

    public native void setInitial(boolean value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setStampColumnsList(JsArray<String> value);

    @JsOverlay
    public final void setStampColumnsList(String[] value) {
        setStampColumnsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setTriggerId();

    public native void setTriggerId(TableReference value);

    public native SnapshotWhenTableRequest.ToObjectReturnType0 toObject();

    public native SnapshotWhenTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

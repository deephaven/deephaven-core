package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.FetchTableRequest",
        namespace = JsPackage.GLOBAL)
public class FetchTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FetchTableRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(FetchTableRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType ticket);
        }

        @JsOverlay
        static FetchTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultId();

        @JsProperty
        FetchTableRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSourceId(FetchTableRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FetchTableRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(
                    FetchTableRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType ticket);
        }

        @JsOverlay
        static FetchTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultId();

        @JsProperty
        FetchTableRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSourceId(FetchTableRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native FetchTableRequest deserializeBinary(Uint8Array bytes);

    public static native FetchTableRequest deserializeBinaryFromReader(
            FetchTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(FetchTableRequest message, Object writer);

    public static native FetchTableRequest.ToObjectReturnType toObject(
            boolean includeInstance, FetchTableRequest msg);

    public native void clearResultId();

    public native void clearSourceId();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native FetchTableRequest.ToObjectReturnType0 toObject();

    public native FetchTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

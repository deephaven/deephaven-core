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
    name = "dhinternal.io.deephaven.proto.table_pb.HeadOrTailRequest",
    namespace = JsPackage.GLOBAL)
public class HeadOrTailRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static HeadOrTailRequest.ToObjectReturnType.SourceIdFieldType create() {
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
        static HeadOrTailRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getNumRows();

        @JsProperty
        HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        HeadOrTailRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        void setNumRows(String numRows);

        @JsProperty
        void setResultId(HeadOrTailRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(HeadOrTailRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static HeadOrTailRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
        static HeadOrTailRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getNumRows();

        @JsProperty
        HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        HeadOrTailRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        void setNumRows(String numRows);

        @JsProperty
        void setResultId(HeadOrTailRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(HeadOrTailRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native HeadOrTailRequest deserializeBinary(Uint8Array bytes);

    public static native HeadOrTailRequest deserializeBinaryFromReader(
        HeadOrTailRequest message, Object reader);

    public static native void serializeBinaryToWriter(HeadOrTailRequest message, Object writer);

    public static native HeadOrTailRequest.ToObjectReturnType toObject(
        boolean includeInstance, HeadOrTailRequest msg);

    public native void clearResultId();

    public native void clearSourceId();

    public native String getNumRows();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setNumRows(String value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native HeadOrTailRequest.ToObjectReturnType0 toObject();

    public native HeadOrTailRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

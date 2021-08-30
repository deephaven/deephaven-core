package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

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
        name = "dhinternal.io.deephaven.proto.session_pb.ExportRequest",
        namespace = JsPackage.GLOBAL)
public class ExportRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ExportRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType of(Object o) {
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
            static ExportRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ExportRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(ExportRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ExportRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ExportRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static ExportRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultId();

        @JsProperty
        ExportRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSourceId(ExportRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ExportRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType of(Object o) {
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
            static ExportRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ExportRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(ExportRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ExportRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ExportRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static ExportRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultId();

        @JsProperty
        ExportRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSourceId(ExportRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native ExportRequest deserializeBinary(Uint8Array bytes);

    public static native ExportRequest deserializeBinaryFromReader(
            ExportRequest message, Object reader);

    public static native void serializeBinaryToWriter(ExportRequest message, Object writer);

    public static native ExportRequest.ToObjectReturnType toObject(
            boolean includeInstance, ExportRequest msg);

    public native void clearResultId();

    public native void clearSourceId();

    public native Ticket getResultId();

    public native Ticket getSourceId();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(Ticket value);

    public native ExportRequest.ToObjectReturnType0 toObject();

    public native ExportRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb;

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
        name = "dhinternal.io.deephaven_core.proto.session_pb.PublishRequest",
        namespace = JsPackage.GLOBAL)
public class PublishRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static PublishRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType of(Object o) {
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
            static PublishRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            PublishRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(PublishRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<PublishRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<PublishRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static PublishRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultId();

        @JsProperty
        PublishRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSourceId(PublishRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static PublishRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType of(
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
            static PublishRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            PublishRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    PublishRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<PublishRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<PublishRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static PublishRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultId();

        @JsProperty
        PublishRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSourceId(PublishRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native PublishRequest deserializeBinary(Uint8Array bytes);

    public static native PublishRequest deserializeBinaryFromReader(
            PublishRequest message, Object reader);

    public static native void serializeBinaryToWriter(PublishRequest message, Object writer);

    public static native PublishRequest.ToObjectReturnType toObject(
            boolean includeInstance, PublishRequest msg);

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

    public native PublishRequest.ToObjectReturnType0 toObject();

    public native PublishRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

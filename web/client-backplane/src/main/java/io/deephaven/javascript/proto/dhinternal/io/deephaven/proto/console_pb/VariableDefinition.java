package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
        name = "dhinternal.io.deephaven.proto.console_pb.VariableDefinition",
        namespace = JsPackage.GLOBAL)
public class VariableDefinition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface IdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static VariableDefinition.ToObjectReturnType.IdFieldType.GetTicketUnionType of(Object o) {
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
            static VariableDefinition.ToObjectReturnType.IdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            VariableDefinition.ToObjectReturnType.IdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(VariableDefinition.ToObjectReturnType.IdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<VariableDefinition.ToObjectReturnType.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<VariableDefinition.ToObjectReturnType.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static VariableDefinition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        VariableDefinition.ToObjectReturnType.IdFieldType getId();

        @JsProperty
        String getTitle();

        @JsProperty
        String getType();

        @JsProperty
        void setId(VariableDefinition.ToObjectReturnType.IdFieldType id);

        @JsProperty
        void setTitle(String title);

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface IdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static VariableDefinition.ToObjectReturnType0.IdFieldType.GetTicketUnionType of(Object o) {
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
            static VariableDefinition.ToObjectReturnType0.IdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            VariableDefinition.ToObjectReturnType0.IdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(VariableDefinition.ToObjectReturnType0.IdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<VariableDefinition.ToObjectReturnType0.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<VariableDefinition.ToObjectReturnType0.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static VariableDefinition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        VariableDefinition.ToObjectReturnType0.IdFieldType getId();

        @JsProperty
        String getTitle();

        @JsProperty
        String getType();

        @JsProperty
        void setId(VariableDefinition.ToObjectReturnType0.IdFieldType id);

        @JsProperty
        void setTitle(String title);

        @JsProperty
        void setType(String type);
    }

    public static native VariableDefinition deserializeBinary(Uint8Array bytes);

    public static native VariableDefinition deserializeBinaryFromReader(
            VariableDefinition message, Object reader);

    public static native void serializeBinaryToWriter(VariableDefinition message, Object writer);

    public static native VariableDefinition.ToObjectReturnType toObject(
            boolean includeInstance, VariableDefinition msg);

    public native void clearId();

    public native Ticket getId();

    public native String getTitle();

    public native String getType();

    public native boolean hasId();

    public native Uint8Array serializeBinary();

    public native void setId();

    public native void setId(Ticket value);

    public native void setTitle(String value);

    public native void setType(String value);

    public native VariableDefinition.ToObjectReturnType0 toObject();

    public native VariableDefinition.ToObjectReturnType0 toObject(boolean includeInstance);
}

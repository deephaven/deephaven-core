package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb.FieldInfo",
        namespace = JsPackage.GLOBAL)
public class FieldInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TypedTicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FieldInfo.ToObjectReturnType.TypedTicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(FieldInfo.ToObjectReturnType.TypedTicketFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static FieldInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getApplicationId();

        @JsProperty
        String getApplicationName();

        @JsProperty
        String getFieldDescription();

        @JsProperty
        String getFieldName();

        @JsProperty
        FieldInfo.ToObjectReturnType.TypedTicketFieldType getTypedTicket();

        @JsProperty
        void setApplicationId(String applicationId);

        @JsProperty
        void setApplicationName(String applicationName);

        @JsProperty
        void setFieldDescription(String fieldDescription);

        @JsProperty
        void setFieldName(String fieldName);

        @JsProperty
        void setTypedTicket(FieldInfo.ToObjectReturnType.TypedTicketFieldType typedTicket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TypedTicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FieldInfo.ToObjectReturnType0.TypedTicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(FieldInfo.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static FieldInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getApplicationId();

        @JsProperty
        String getApplicationName();

        @JsProperty
        String getFieldDescription();

        @JsProperty
        String getFieldName();

        @JsProperty
        FieldInfo.ToObjectReturnType0.TypedTicketFieldType getTypedTicket();

        @JsProperty
        void setApplicationId(String applicationId);

        @JsProperty
        void setApplicationName(String applicationName);

        @JsProperty
        void setFieldDescription(String fieldDescription);

        @JsProperty
        void setFieldName(String fieldName);

        @JsProperty
        void setTypedTicket(FieldInfo.ToObjectReturnType0.TypedTicketFieldType typedTicket);
    }

    public static native FieldInfo deserializeBinary(Uint8Array bytes);

    public static native FieldInfo deserializeBinaryFromReader(FieldInfo message, Object reader);

    public static native void serializeBinaryToWriter(FieldInfo message, Object writer);

    public static native FieldInfo.ToObjectReturnType toObject(
            boolean includeInstance, FieldInfo msg);

    public native void clearTypedTicket();

    public native String getApplicationId();

    public native String getApplicationName();

    public native String getFieldDescription();

    public native String getFieldName();

    public native TypedTicket getTypedTicket();

    public native boolean hasTypedTicket();

    public native Uint8Array serializeBinary();

    public native void setApplicationId(String value);

    public native void setApplicationName(String value);

    public native void setFieldDescription(String value);

    public native void setFieldName(String value);

    public native void setTypedTicket();

    public native void setTypedTicket(TypedTicket value);

    public native FieldInfo.ToObjectReturnType0 toObject();

    public native FieldInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}

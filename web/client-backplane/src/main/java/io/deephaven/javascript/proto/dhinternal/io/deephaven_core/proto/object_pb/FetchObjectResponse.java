//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.object_pb.FetchObjectResponse",
        namespace = JsPackage.GLOBAL)
public class FetchObjectResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetDataUnionType {
        @JsOverlay
        static FetchObjectResponse.GetDataUnionType of(Object o) {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetDataValueUnionType {
        @JsOverlay
        static FetchObjectResponse.SetDataValueUnionType of(Object o) {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetDataUnionType {
            @JsOverlay
            static FetchObjectResponse.ToObjectReturnType.GetDataUnionType of(Object o) {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TypedExportIdsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static FetchObjectResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchObjectResponse.ToObjectReturnType.GetDataUnionType getData();

        @JsProperty
        String getType();

        @JsProperty
        JsArray<FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType> getTypedExportIdsList();

        @JsProperty
        void setData(FetchObjectResponse.ToObjectReturnType.GetDataUnionType data);

        @JsOverlay
        default void setData(String data) {
            setData(Js.<FetchObjectResponse.ToObjectReturnType.GetDataUnionType>uncheckedCast(data));
        }

        @JsOverlay
        default void setData(Uint8Array data) {
            setData(Js.<FetchObjectResponse.ToObjectReturnType.GetDataUnionType>uncheckedCast(data));
        }

        @JsProperty
        void setType(String type);

        @JsProperty
        void setTypedExportIdsList(
                JsArray<FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType> typedExportIdsList);

        @JsOverlay
        default void setTypedExportIdsList(
                FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType[] typedExportIdsList) {
            setTypedExportIdsList(
                    Js.<JsArray<FetchObjectResponse.ToObjectReturnType.TypedExportIdsListFieldType>>uncheckedCast(
                            typedExportIdsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetDataUnionType {
            @JsOverlay
            static FetchObjectResponse.ToObjectReturnType0.GetDataUnionType of(Object o) {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TypedExportIdsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static FetchObjectResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchObjectResponse.ToObjectReturnType0.GetDataUnionType getData();

        @JsProperty
        String getType();

        @JsProperty
        JsArray<FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType> getTypedExportIdsList();

        @JsProperty
        void setData(FetchObjectResponse.ToObjectReturnType0.GetDataUnionType data);

        @JsOverlay
        default void setData(String data) {
            setData(Js.<FetchObjectResponse.ToObjectReturnType0.GetDataUnionType>uncheckedCast(data));
        }

        @JsOverlay
        default void setData(Uint8Array data) {
            setData(Js.<FetchObjectResponse.ToObjectReturnType0.GetDataUnionType>uncheckedCast(data));
        }

        @JsProperty
        void setType(String type);

        @JsProperty
        void setTypedExportIdsList(
                JsArray<FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType> typedExportIdsList);

        @JsOverlay
        default void setTypedExportIdsList(
                FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType[] typedExportIdsList) {
            setTypedExportIdsList(
                    Js.<JsArray<FetchObjectResponse.ToObjectReturnType0.TypedExportIdsListFieldType>>uncheckedCast(
                            typedExportIdsList));
        }
    }

    public static native FetchObjectResponse deserializeBinary(Uint8Array bytes);

    public static native FetchObjectResponse deserializeBinaryFromReader(
            FetchObjectResponse message, Object reader);

    public static native void serializeBinaryToWriter(FetchObjectResponse message, Object writer);

    public static native FetchObjectResponse.ToObjectReturnType toObject(
            boolean includeInstance, FetchObjectResponse msg);

    public native TypedTicket addTypedExportIds();

    public native TypedTicket addTypedExportIds(TypedTicket value, double index);

    public native TypedTicket addTypedExportIds(TypedTicket value);

    public native void clearTypedExportIdsList();

    public native FetchObjectResponse.GetDataUnionType getData();

    public native String getData_asB64();

    public native Uint8Array getData_asU8();

    public native String getType();

    public native JsArray<TypedTicket> getTypedExportIdsList();

    public native Uint8Array serializeBinary();

    public native void setData(FetchObjectResponse.SetDataValueUnionType value);

    @JsOverlay
    public final void setData(String value) {
        setData(Js.<FetchObjectResponse.SetDataValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setData(Uint8Array value) {
        setData(Js.<FetchObjectResponse.SetDataValueUnionType>uncheckedCast(value));
    }

    public native void setType(String value);

    public native void setTypedExportIdsList(JsArray<TypedTicket> value);

    @JsOverlay
    public final void setTypedExportIdsList(TypedTicket[] value) {
        setTypedExportIdsList(Js.<JsArray<TypedTicket>>uncheckedCast(value));
    }

    public native FetchObjectResponse.ToObjectReturnType0 toObject();

    public native FetchObjectResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb;

import elemental2.core.JsArray;
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
        name = "dhinternal.io.deephaven.proto.object_pb.Data",
        namespace = JsPackage.GLOBAL)
public class Data {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetPayloadUnionType {
        @JsOverlay
        static Data.GetPayloadUnionType of(Object o) {
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
    public interface SetPayloadValueUnionType {
        @JsOverlay
        static Data.SetPayloadValueUnionType of(Object o) {
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
        public interface ExportedReferencesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static Data.ToObjectReturnType.ExportedReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    Data.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static Data.ToObjectReturnType.GetPayloadUnionType of(Object o) {
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
        static Data.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Data.ToObjectReturnType.ExportedReferencesListFieldType> getExportedReferencesList();

        @JsProperty
        Data.ToObjectReturnType.GetPayloadUnionType getPayload();

        @JsOverlay
        default void setExportedReferencesList(
                Data.ToObjectReturnType.ExportedReferencesListFieldType[] exportedReferencesList) {
            setExportedReferencesList(
                    Js.<JsArray<Data.ToObjectReturnType.ExportedReferencesListFieldType>>uncheckedCast(
                            exportedReferencesList));
        }

        @JsProperty
        void setExportedReferencesList(
                JsArray<Data.ToObjectReturnType.ExportedReferencesListFieldType> exportedReferencesList);

        @JsProperty
        void setPayload(Data.ToObjectReturnType.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<Data.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<Data.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ExportedReferencesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static Data.ToObjectReturnType0.ExportedReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    Data.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static Data.ToObjectReturnType0.GetPayloadUnionType of(Object o) {
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
        static Data.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Data.ToObjectReturnType0.ExportedReferencesListFieldType> getExportedReferencesList();

        @JsProperty
        Data.ToObjectReturnType0.GetPayloadUnionType getPayload();

        @JsOverlay
        default void setExportedReferencesList(
                Data.ToObjectReturnType0.ExportedReferencesListFieldType[] exportedReferencesList) {
            setExportedReferencesList(
                    Js.<JsArray<Data.ToObjectReturnType0.ExportedReferencesListFieldType>>uncheckedCast(
                            exportedReferencesList));
        }

        @JsProperty
        void setExportedReferencesList(
                JsArray<Data.ToObjectReturnType0.ExportedReferencesListFieldType> exportedReferencesList);

        @JsProperty
        void setPayload(Data.ToObjectReturnType0.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<Data.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<Data.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }
    }

    public static native Data deserializeBinary(Uint8Array bytes);

    public static native Data deserializeBinaryFromReader(Data message, Object reader);

    public static native void serializeBinaryToWriter(Data message, Object writer);

    public static native Data.ToObjectReturnType toObject(boolean includeInstance, Data msg);

    public native TypedTicket addExportedReferences();

    public native TypedTicket addExportedReferences(TypedTicket value, double index);

    public native TypedTicket addExportedReferences(TypedTicket value);

    public native void clearExportedReferencesList();

    public native JsArray<TypedTicket> getExportedReferencesList();

    public native Data.GetPayloadUnionType getPayload();

    public native String getPayload_asB64();

    public native Uint8Array getPayload_asU8();

    public native Uint8Array serializeBinary();

    public native void setExportedReferencesList(JsArray<TypedTicket> value);

    @JsOverlay
    public final void setExportedReferencesList(TypedTicket[] value) {
        setExportedReferencesList(Js.<JsArray<TypedTicket>>uncheckedCast(value));
    }

    public native void setPayload(Data.SetPayloadValueUnionType value);

    @JsOverlay
    public final void setPayload(String value) {
        setPayload(Js.<Data.SetPayloadValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setPayload(Uint8Array value) {
        setPayload(Js.<Data.SetPayloadValueUnionType>uncheckedCast(value));
    }

    public native Data.ToObjectReturnType0 toObject();

    public native Data.ToObjectReturnType0 toObject(boolean includeInstance);
}

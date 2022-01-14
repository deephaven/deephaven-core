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
        name = "dhinternal.io.deephaven.proto.object_pb.FetchObjectResponse",
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
        public interface ExportIdListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType.GetTicketUnionType of(
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
            static FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType.GetTicketUnionType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsProperty
            void setType(String type);
        }

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

        @JsOverlay
        static FetchObjectResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchObjectResponse.ToObjectReturnType.GetDataUnionType getData();

        @JsProperty
        JsArray<FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType> getExportIdList();

        @JsProperty
        String getType();

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

        @JsOverlay
        default void setExportIdList(
                FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType[] exportIdList) {
            setExportIdList(
                    Js.<JsArray<FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType>>uncheckedCast(
                            exportIdList));
        }

        @JsProperty
        void setExportIdList(
                JsArray<FetchObjectResponse.ToObjectReturnType.ExportIdListFieldType> exportIdList);

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ExportIdListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType.GetTicketUnionType of(
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
            static FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType.GetTicketUnionType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsProperty
            void setType(String type);
        }

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

        @JsOverlay
        static FetchObjectResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchObjectResponse.ToObjectReturnType0.GetDataUnionType getData();

        @JsProperty
        JsArray<FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType> getExportIdList();

        @JsProperty
        String getType();

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

        @JsOverlay
        default void setExportIdList(
                FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType[] exportIdList) {
            setExportIdList(
                    Js.<JsArray<FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType>>uncheckedCast(
                            exportIdList));
        }

        @JsProperty
        void setExportIdList(
                JsArray<FetchObjectResponse.ToObjectReturnType0.ExportIdListFieldType> exportIdList);

        @JsProperty
        void setType(String type);
    }

    public static native FetchObjectResponse deserializeBinary(Uint8Array bytes);

    public static native FetchObjectResponse deserializeBinaryFromReader(
            FetchObjectResponse message, Object reader);

    public static native void serializeBinaryToWriter(FetchObjectResponse message, Object writer);

    public static native FetchObjectResponse.ToObjectReturnType toObject(
            boolean includeInstance, FetchObjectResponse msg);

    public native TypedTicket addExportId();

    public native TypedTicket addExportId(TypedTicket value, double index);

    public native TypedTicket addExportId(TypedTicket value);

    public native void clearExportIdList();

    public native FetchObjectResponse.GetDataUnionType getData();

    public native String getData_asB64();

    public native Uint8Array getData_asU8();

    public native JsArray<TypedTicket> getExportIdList();

    public native String getType();

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

    public native void setExportIdList(JsArray<TypedTicket> value);

    @JsOverlay
    public final void setExportIdList(TypedTicket[] value) {
        setExportIdList(Js.<JsArray<TypedTicket>>uncheckedCast(value));
    }

    public native void setType(String value);

    public native FetchObjectResponse.ToObjectReturnType0 toObject();

    public native FetchObjectResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}

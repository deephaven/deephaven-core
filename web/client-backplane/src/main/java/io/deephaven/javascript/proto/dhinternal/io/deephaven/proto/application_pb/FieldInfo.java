package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.fieldinfo.FieldType;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
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
        public interface FieldFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CustomFieldType {
                @JsOverlay
                static FieldInfo.ToObjectReturnType.FieldFieldType.CustomFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getType();

                @JsProperty
                void setType(String type);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TableFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetSchemaHeaderUnionType {
                    @JsOverlay
                    static FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType of(
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
                static FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType getSchemaHeader();

                @JsProperty
                String getSize();

                @JsProperty
                boolean isIsStatic();

                @JsProperty
                void setIsStatic(boolean isStatic);

                @JsProperty
                void setSchemaHeader(
                        FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType schemaHeader);

                @JsOverlay
                default void setSchemaHeader(String schemaHeader) {
                    setSchemaHeader(
                            Js.<FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                    schemaHeader));
                }

                @JsOverlay
                default void setSchemaHeader(Uint8Array schemaHeader) {
                    setSchemaHeader(
                            Js.<FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                    schemaHeader));
                }

                @JsProperty
                void setSize(String size);
            }

            @JsOverlay
            static FieldInfo.ToObjectReturnType.FieldFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FieldInfo.ToObjectReturnType.FieldFieldType.CustomFieldType getCustom();

            @JsProperty
            Object getFigure();

            @JsProperty
            FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType getTable();

            @JsProperty
            void setCustom(FieldInfo.ToObjectReturnType.FieldFieldType.CustomFieldType custom);

            @JsProperty
            void setFigure(Object figure);

            @JsProperty
            void setTable(FieldInfo.ToObjectReturnType.FieldFieldType.TableFieldType table);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FieldInfo.ToObjectReturnType.TicketFieldType.GetTicketUnionType of(Object o) {
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
            static FieldInfo.ToObjectReturnType.TicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FieldInfo.ToObjectReturnType.TicketFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(FieldInfo.ToObjectReturnType.TicketFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<FieldInfo.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<FieldInfo.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
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
        FieldInfo.ToObjectReturnType.FieldFieldType getFieldType();

        @JsProperty
        FieldInfo.ToObjectReturnType.TicketFieldType getTicket();

        @JsProperty
        void setApplicationId(String applicationId);

        @JsProperty
        void setApplicationName(String applicationName);

        @JsProperty
        void setFieldDescription(String fieldDescription);

        @JsProperty
        void setFieldName(String fieldName);

        @JsProperty
        void setFieldType(FieldInfo.ToObjectReturnType.FieldFieldType fieldType);

        @JsProperty
        void setTicket(FieldInfo.ToObjectReturnType.TicketFieldType ticket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FieldFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CustomFieldType {
                @JsOverlay
                static FieldInfo.ToObjectReturnType0.FieldFieldType.CustomFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getType();

                @JsProperty
                void setType(String type);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TableFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetSchemaHeaderUnionType {
                    @JsOverlay
                    static FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType of(
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
                static FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType getSchemaHeader();

                @JsProperty
                String getSize();

                @JsProperty
                boolean isIsStatic();

                @JsProperty
                void setIsStatic(boolean isStatic);

                @JsProperty
                void setSchemaHeader(
                        FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType schemaHeader);

                @JsOverlay
                default void setSchemaHeader(String schemaHeader) {
                    setSchemaHeader(
                            Js.<FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                    schemaHeader));
                }

                @JsOverlay
                default void setSchemaHeader(Uint8Array schemaHeader) {
                    setSchemaHeader(
                            Js.<FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                    schemaHeader));
                }

                @JsProperty
                void setSize(String size);
            }

            @JsOverlay
            static FieldInfo.ToObjectReturnType0.FieldFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FieldInfo.ToObjectReturnType0.FieldFieldType.CustomFieldType getCustom();

            @JsProperty
            Object getFigure();

            @JsProperty
            FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType getTable();

            @JsProperty
            void setCustom(FieldInfo.ToObjectReturnType0.FieldFieldType.CustomFieldType custom);

            @JsProperty
            void setFigure(Object figure);

            @JsProperty
            void setTable(FieldInfo.ToObjectReturnType0.FieldFieldType.TableFieldType table);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FieldInfo.ToObjectReturnType0.TicketFieldType.GetTicketUnionType of(Object o) {
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
            static FieldInfo.ToObjectReturnType0.TicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FieldInfo.ToObjectReturnType0.TicketFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(FieldInfo.ToObjectReturnType0.TicketFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<FieldInfo.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<FieldInfo.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
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
        FieldInfo.ToObjectReturnType0.FieldFieldType getFieldType();

        @JsProperty
        FieldInfo.ToObjectReturnType0.TicketFieldType getTicket();

        @JsProperty
        void setApplicationId(String applicationId);

        @JsProperty
        void setApplicationName(String applicationName);

        @JsProperty
        void setFieldDescription(String fieldDescription);

        @JsProperty
        void setFieldName(String fieldName);

        @JsProperty
        void setFieldType(FieldInfo.ToObjectReturnType0.FieldFieldType fieldType);

        @JsProperty
        void setTicket(FieldInfo.ToObjectReturnType0.TicketFieldType ticket);
    }

    public static native FieldInfo deserializeBinary(Uint8Array bytes);

    public static native FieldInfo deserializeBinaryFromReader(FieldInfo message, Object reader);

    public static native void serializeBinaryToWriter(FieldInfo message, Object writer);

    public static native FieldInfo.ToObjectReturnType toObject(
            boolean includeInstance, FieldInfo msg);

    public native void clearFieldType();

    public native void clearTicket();

    public native String getApplicationId();

    public native String getApplicationName();

    public native String getFieldDescription();

    public native String getFieldName();

    public native FieldType getFieldType();

    public native Ticket getTicket();

    public native boolean hasFieldType();

    public native boolean hasTicket();

    public native Uint8Array serializeBinary();

    public native void setApplicationId(String value);

    public native void setApplicationName(String value);

    public native void setFieldDescription(String value);

    public native void setFieldName(String value);

    public native void setFieldType();

    public native void setFieldType(FieldType value);

    public native void setTicket();

    public native void setTicket(Ticket value);

    public native FieldInfo.ToObjectReturnType0 toObject();

    public native FieldInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}

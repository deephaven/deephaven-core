package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb.FieldsChangeUpdate",
        namespace = JsPackage.GLOBAL)
public class FieldsChangeUpdate {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CreatedListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FieldFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface CustomFieldType {
                    @JsOverlay
                    static FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.CustomFieldType create() {
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
                        static FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType of(
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
                    static FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType getSchemaHeader();

                    @JsProperty
                    String getSize();

                    @JsProperty
                    boolean isIsStatic();

                    @JsProperty
                    void setIsStatic(boolean isStatic);

                    @JsProperty
                    void setSchemaHeader(
                            FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType schemaHeader);

                    @JsOverlay
                    default void setSchemaHeader(String schemaHeader) {
                        setSchemaHeader(
                                Js.<FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                        schemaHeader));
                    }

                    @JsOverlay
                    default void setSchemaHeader(Uint8Array schemaHeader) {
                        setSchemaHeader(
                                Js.<FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                        schemaHeader));
                    }

                    @JsProperty
                    void setSize(String size);
                }

                @JsOverlay
                static FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.CustomFieldType getCustom();

                @JsProperty
                Object getFigure();

                @JsProperty
                FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType getTable();

                @JsProperty
                void setCustom(
                        FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.CustomFieldType custom);

                @JsProperty
                void setFigure(Object figure);

                @JsProperty
                void setTable(
                        FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType.TableFieldType table);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType create() {
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
            FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType getFieldType();

            @JsProperty
            FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType getTicket();

            @JsProperty
            void setApplicationId(String applicationId);

            @JsProperty
            void setApplicationName(String applicationName);

            @JsProperty
            void setFieldDescription(String fieldDescription);

            @JsProperty
            void setFieldName(String fieldName);

            @JsProperty
            void setFieldType(
                    FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.FieldFieldType fieldType);

            @JsProperty
            void setTicket(
                    FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType.TicketFieldType ticket);
        }

        @JsOverlay
        static FieldsChangeUpdate.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType> getCreatedList();

        @JsProperty
        JsArray<Object> getRemovedList();

        @JsProperty
        JsArray<Object> getUpdatedList();

        @JsOverlay
        default void setCreatedList(
                FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType[] createdList) {
            setCreatedList(
                    Js.<JsArray<FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType>>uncheckedCast(
                            createdList));
        }

        @JsProperty
        void setCreatedList(
                JsArray<FieldsChangeUpdate.ToObjectReturnType.CreatedListFieldType> createdList);

        @JsProperty
        void setRemovedList(JsArray<Object> removedList);

        @JsOverlay
        default void setRemovedList(Object[] removedList) {
            setRemovedList(Js.<JsArray<Object>>uncheckedCast(removedList));
        }

        @JsProperty
        void setUpdatedList(JsArray<Object> updatedList);

        @JsOverlay
        default void setUpdatedList(Object[] updatedList) {
            setUpdatedList(Js.<JsArray<Object>>uncheckedCast(updatedList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CreatedListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FieldFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface CustomFieldType {
                    @JsOverlay
                    static FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.CustomFieldType create() {
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
                        static FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType of(
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
                    static FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType getSchemaHeader();

                    @JsProperty
                    String getSize();

                    @JsProperty
                    boolean isIsStatic();

                    @JsProperty
                    void setIsStatic(boolean isStatic);

                    @JsProperty
                    void setSchemaHeader(
                            FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType schemaHeader);

                    @JsOverlay
                    default void setSchemaHeader(String schemaHeader) {
                        setSchemaHeader(
                                Js.<FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                        schemaHeader));
                    }

                    @JsOverlay
                    default void setSchemaHeader(Uint8Array schemaHeader) {
                        setSchemaHeader(
                                Js.<FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                        schemaHeader));
                    }

                    @JsProperty
                    void setSize(String size);
                }

                @JsOverlay
                static FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.CustomFieldType getCustom();

                @JsProperty
                Object getFigure();

                @JsProperty
                FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType getTable();

                @JsProperty
                void setCustom(
                        FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.CustomFieldType custom);

                @JsProperty
                void setFigure(Object figure);

                @JsProperty
                void setTable(
                        FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType.TableFieldType table);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType create() {
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
            FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType getFieldType();

            @JsProperty
            FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType getTicket();

            @JsProperty
            void setApplicationId(String applicationId);

            @JsProperty
            void setApplicationName(String applicationName);

            @JsProperty
            void setFieldDescription(String fieldDescription);

            @JsProperty
            void setFieldName(String fieldName);

            @JsProperty
            void setFieldType(
                    FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.FieldFieldType fieldType);

            @JsProperty
            void setTicket(
                    FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType.TicketFieldType ticket);
        }

        @JsOverlay
        static FieldsChangeUpdate.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType> getCreatedList();

        @JsProperty
        JsArray<Object> getRemovedList();

        @JsProperty
        JsArray<Object> getUpdatedList();

        @JsOverlay
        default void setCreatedList(
                FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType[] createdList) {
            setCreatedList(
                    Js.<JsArray<FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType>>uncheckedCast(
                            createdList));
        }

        @JsProperty
        void setCreatedList(
                JsArray<FieldsChangeUpdate.ToObjectReturnType0.CreatedListFieldType> createdList);

        @JsProperty
        void setRemovedList(JsArray<Object> removedList);

        @JsOverlay
        default void setRemovedList(Object[] removedList) {
            setRemovedList(Js.<JsArray<Object>>uncheckedCast(removedList));
        }

        @JsProperty
        void setUpdatedList(JsArray<Object> updatedList);

        @JsOverlay
        default void setUpdatedList(Object[] updatedList) {
            setUpdatedList(Js.<JsArray<Object>>uncheckedCast(updatedList));
        }
    }

    public static native FieldsChangeUpdate deserializeBinary(Uint8Array bytes);

    public static native FieldsChangeUpdate deserializeBinaryFromReader(
            FieldsChangeUpdate message, Object reader);

    public static native void serializeBinaryToWriter(FieldsChangeUpdate message, Object writer);

    public static native FieldsChangeUpdate.ToObjectReturnType toObject(
            boolean includeInstance, FieldsChangeUpdate msg);

    public native FieldInfo addCreated();

    public native FieldInfo addCreated(FieldInfo value, double index);

    public native FieldInfo addCreated(FieldInfo value);

    public native FieldInfo addRemoved();

    public native FieldInfo addRemoved(FieldInfo value, double index);

    public native FieldInfo addRemoved(FieldInfo value);

    public native FieldInfo addUpdated();

    public native FieldInfo addUpdated(FieldInfo value, double index);

    public native FieldInfo addUpdated(FieldInfo value);

    public native void clearCreatedList();

    public native void clearRemovedList();

    public native void clearUpdatedList();

    public native JsArray<FieldInfo> getCreatedList();

    public native JsArray<FieldInfo> getRemovedList();

    public native JsArray<FieldInfo> getUpdatedList();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setCreatedList(FieldInfo[] value) {
        setCreatedList(Js.<JsArray<FieldInfo>>uncheckedCast(value));
    }

    public native void setCreatedList(JsArray<FieldInfo> value);

    @JsOverlay
    public final void setRemovedList(FieldInfo[] value) {
        setRemovedList(Js.<JsArray<FieldInfo>>uncheckedCast(value));
    }

    public native void setRemovedList(JsArray<FieldInfo> value);

    @JsOverlay
    public final void setUpdatedList(FieldInfo[] value) {
        setUpdatedList(Js.<JsArray<FieldInfo>>uncheckedCast(value));
    }

    public native void setUpdatedList(JsArray<FieldInfo> value);

    public native FieldsChangeUpdate.ToObjectReturnType0 toObject();

    public native FieldsChangeUpdate.ToObjectReturnType0 toObject(boolean includeInstance);
}

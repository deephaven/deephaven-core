package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldsChangeUpdate;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.ExecuteCommandResponse",
        namespace = JsPackage.GLOBAL)
public class ExecuteCommandResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ChangesFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CreatedListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TypedTicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TicketFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface GetTicketUnionType {
                            @JsOverlay
                            static ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                        static ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                        @JsProperty
                        void setTicket(
                                ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                        @JsOverlay
                        default void setTicket(String ticket) {
                            setTicket(
                                    Js.<ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                            ticket));
                        }

                        @JsOverlay
                        default void setTicket(Uint8Array ticket) {
                            setTicket(
                                    Js.<ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                            ticket));
                        }
                    }

                    @JsOverlay
                    static ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType getTicket();

                    @JsProperty
                    String getType();

                    @JsProperty
                    void setTicket(
                            ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType ticket);

                    @JsProperty
                    void setType(String type);
                }

                @JsOverlay
                static ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType create() {
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
                ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType getTypedTicket();

                @JsProperty
                void setApplicationId(String applicationId);

                @JsProperty
                void setApplicationName(String applicationName);

                @JsProperty
                void setFieldDescription(String fieldDescription);

                @JsProperty
                void setFieldName(String fieldName);

                @JsProperty
                void setTypedTicket(
                        ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType typedTicket);
            }

            @JsOverlay
            static ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType> getCreatedList();

            @JsProperty
            JsArray<Object> getRemovedList();

            @JsProperty
            JsArray<Object> getUpdatedList();

            @JsOverlay
            default void setCreatedList(
                    ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType[] createdList) {
                setCreatedList(
                        Js.<JsArray<ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType>>uncheckedCast(
                                createdList));
            }

            @JsProperty
            void setCreatedList(
                    JsArray<ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType.CreatedListFieldType> createdList);

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

        @JsOverlay
        static ExecuteCommandResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType getChanges();

        @JsProperty
        String getErrorMessage();

        @JsProperty
        void setChanges(ExecuteCommandResponse.ToObjectReturnType.ChangesFieldType changes);

        @JsProperty
        void setErrorMessage(String errorMessage);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ChangesFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CreatedListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TypedTicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TicketFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface GetTicketUnionType {
                            @JsOverlay
                            static ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                        static ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                        @JsProperty
                        void setTicket(
                                ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                        @JsOverlay
                        default void setTicket(String ticket) {
                            setTicket(
                                    Js.<ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                            ticket));
                        }

                        @JsOverlay
                        default void setTicket(Uint8Array ticket) {
                            setTicket(
                                    Js.<ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                            ticket));
                        }
                    }

                    @JsOverlay
                    static ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType getTicket();

                    @JsProperty
                    String getType();

                    @JsProperty
                    void setTicket(
                            ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType.TicketFieldType ticket);

                    @JsProperty
                    void setType(String type);
                }

                @JsOverlay
                static ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType create() {
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
                ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType getTypedTicket();

                @JsProperty
                void setApplicationId(String applicationId);

                @JsProperty
                void setApplicationName(String applicationName);

                @JsProperty
                void setFieldDescription(String fieldDescription);

                @JsProperty
                void setFieldName(String fieldName);

                @JsProperty
                void setTypedTicket(
                        ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType.TypedTicketFieldType typedTicket);
            }

            @JsOverlay
            static ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType> getCreatedList();

            @JsProperty
            JsArray<Object> getRemovedList();

            @JsProperty
            JsArray<Object> getUpdatedList();

            @JsOverlay
            default void setCreatedList(
                    ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType[] createdList) {
                setCreatedList(
                        Js.<JsArray<ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType>>uncheckedCast(
                                createdList));
            }

            @JsProperty
            void setCreatedList(
                    JsArray<ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType.CreatedListFieldType> createdList);

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

        @JsOverlay
        static ExecuteCommandResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType getChanges();

        @JsProperty
        String getErrorMessage();

        @JsProperty
        void setChanges(ExecuteCommandResponse.ToObjectReturnType0.ChangesFieldType changes);

        @JsProperty
        void setErrorMessage(String errorMessage);
    }

    public static native ExecuteCommandResponse deserializeBinary(Uint8Array bytes);

    public static native ExecuteCommandResponse deserializeBinaryFromReader(
            ExecuteCommandResponse message, Object reader);

    public static native void serializeBinaryToWriter(ExecuteCommandResponse message, Object writer);

    public static native ExecuteCommandResponse.ToObjectReturnType toObject(
            boolean includeInstance, ExecuteCommandResponse msg);

    public native void clearChanges();

    public native FieldsChangeUpdate getChanges();

    public native String getErrorMessage();

    public native boolean hasChanges();

    public native Uint8Array serializeBinary();

    public native void setChanges();

    public native void setChanges(FieldsChangeUpdate value);

    public native void setErrorMessage(String value);

    public native ExecuteCommandResponse.ToObjectReturnType0 toObject();

    public native ExecuteCommandResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}

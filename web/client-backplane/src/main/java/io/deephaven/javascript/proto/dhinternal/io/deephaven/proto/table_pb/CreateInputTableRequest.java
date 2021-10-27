package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest.DefinitionCase;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest.InputTableKind;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.CreateInputTableRequest",
        namespace = JsPackage.GLOBAL)
public class CreateInputTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaUnionType {
        @JsOverlay
        static CreateInputTableRequest.GetSchemaUnionType of(Object o) {
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
    public interface SetSchemaValueUnionType {
        @JsOverlay
        static CreateInputTableRequest.SetSchemaValueUnionType of(Object o) {
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
        public interface GetSchemaUnionType {
            @JsOverlay
            static CreateInputTableRequest.ToObjectReturnType.GetSchemaUnionType of(Object o) {
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
        public interface KindFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface InMemoryKeyBackedFieldType {
                @JsOverlay
                static CreateInputTableRequest.ToObjectReturnType.KindFieldType.InMemoryKeyBackedFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getKeyColumnsList();

                @JsProperty
                void setKeyColumnsList(JsArray<String> keyColumnsList);

                @JsOverlay
                default void setKeyColumnsList(String[] keyColumnsList) {
                    setKeyColumnsList(Js.<JsArray<String>>uncheckedCast(keyColumnsList));
                }
            }

            @JsOverlay
            static CreateInputTableRequest.ToObjectReturnType.KindFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getInMemoryAppendOnly();

            @JsProperty
            CreateInputTableRequest.ToObjectReturnType.KindFieldType.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

            @JsProperty
            void setInMemoryAppendOnly(Object inMemoryAppendOnly);

            @JsProperty
            void setInMemoryKeyBacked(
                    CreateInputTableRequest.ToObjectReturnType.KindFieldType.InMemoryKeyBackedFieldType inMemoryKeyBacked);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceTableIdFieldType {
            @JsOverlay
            static CreateInputTableRequest.ToObjectReturnType.SourceTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsOverlay
        static CreateInputTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType.KindFieldType getKind();

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType.GetSchemaUnionType getSchema();

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType.SourceTableIdFieldType getSourceTableId();

        @JsProperty
        void setKind(CreateInputTableRequest.ToObjectReturnType.KindFieldType kind);

        @JsProperty
        void setResultId(CreateInputTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSchema(CreateInputTableRequest.ToObjectReturnType.GetSchemaUnionType schema);

        @JsOverlay
        default void setSchema(String schema) {
            setSchema(
                    Js.<CreateInputTableRequest.ToObjectReturnType.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsOverlay
        default void setSchema(Uint8Array schema) {
            setSchema(
                    Js.<CreateInputTableRequest.ToObjectReturnType.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsProperty
        void setSourceTableId(
                CreateInputTableRequest.ToObjectReturnType.SourceTableIdFieldType sourceTableId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSchemaUnionType {
            @JsOverlay
            static CreateInputTableRequest.ToObjectReturnType0.GetSchemaUnionType of(Object o) {
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
        public interface KindFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface InMemoryKeyBackedFieldType {
                @JsOverlay
                static CreateInputTableRequest.ToObjectReturnType0.KindFieldType.InMemoryKeyBackedFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getKeyColumnsList();

                @JsProperty
                void setKeyColumnsList(JsArray<String> keyColumnsList);

                @JsOverlay
                default void setKeyColumnsList(String[] keyColumnsList) {
                    setKeyColumnsList(Js.<JsArray<String>>uncheckedCast(keyColumnsList));
                }
            }

            @JsOverlay
            static CreateInputTableRequest.ToObjectReturnType0.KindFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getInMemoryAppendOnly();

            @JsProperty
            CreateInputTableRequest.ToObjectReturnType0.KindFieldType.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

            @JsProperty
            void setInMemoryAppendOnly(Object inMemoryAppendOnly);

            @JsProperty
            void setInMemoryKeyBacked(
                    CreateInputTableRequest.ToObjectReturnType0.KindFieldType.InMemoryKeyBackedFieldType inMemoryKeyBacked);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceTableIdFieldType {
            @JsOverlay
            static CreateInputTableRequest.ToObjectReturnType0.SourceTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsOverlay
        static CreateInputTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType0.KindFieldType getKind();

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType0.GetSchemaUnionType getSchema();

        @JsProperty
        CreateInputTableRequest.ToObjectReturnType0.SourceTableIdFieldType getSourceTableId();

        @JsProperty
        void setKind(CreateInputTableRequest.ToObjectReturnType0.KindFieldType kind);

        @JsProperty
        void setResultId(CreateInputTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSchema(CreateInputTableRequest.ToObjectReturnType0.GetSchemaUnionType schema);

        @JsOverlay
        default void setSchema(String schema) {
            setSchema(
                    Js.<CreateInputTableRequest.ToObjectReturnType0.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsOverlay
        default void setSchema(Uint8Array schema) {
            setSchema(
                    Js.<CreateInputTableRequest.ToObjectReturnType0.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsProperty
        void setSourceTableId(
                CreateInputTableRequest.ToObjectReturnType0.SourceTableIdFieldType sourceTableId);
    }

    public static native CreateInputTableRequest deserializeBinary(Uint8Array bytes);

    public static native CreateInputTableRequest deserializeBinaryFromReader(
            CreateInputTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(CreateInputTableRequest message, Object writer);

    public static native CreateInputTableRequest.ToObjectReturnType toObject(
            boolean includeInstance, CreateInputTableRequest msg);

    public native void clearKind();

    public native void clearResultId();

    public native void clearSchema();

    public native void clearSourceTableId();

    public native DefinitionCase getDefinitionCase();

    public native InputTableKind getKind();

    public native Ticket getResultId();

    public native CreateInputTableRequest.GetSchemaUnionType getSchema();

    public native String getSchema_asB64();

    public native Uint8Array getSchema_asU8();

    public native TableReference getSourceTableId();

    public native boolean hasKind();

    public native boolean hasResultId();

    public native boolean hasSchema();

    public native boolean hasSourceTableId();

    public native Uint8Array serializeBinary();

    public native void setKind();

    public native void setKind(InputTableKind value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSchema(CreateInputTableRequest.SetSchemaValueUnionType value);

    @JsOverlay
    public final void setSchema(String value) {
        setSchema(Js.<CreateInputTableRequest.SetSchemaValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setSchema(Uint8Array value) {
        setSchema(Js.<CreateInputTableRequest.SetSchemaValueUnionType>uncheckedCast(value));
    }

    public native void setSourceTableId();

    public native void setSourceTableId(TableReference value);

    public native CreateInputTableRequest.ToObjectReturnType0 toObject();

    public native CreateInputTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

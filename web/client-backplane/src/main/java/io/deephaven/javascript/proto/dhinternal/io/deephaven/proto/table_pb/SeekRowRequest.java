package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.SeekRowRequest",
        namespace = JsPackage.GLOBAL)
public class SeekRowRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                static SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static SeekRowRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(SeekRowRequest.ToObjectReturnType.SourceIdFieldType.TicketFieldType ticket);
        }

        @JsOverlay
        static SeekRowRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        String getSeekValue();

        @JsProperty
        SeekRowRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        String getStartingRow();

        @JsProperty
        String getValueType();

        @JsProperty
        boolean isContains();

        @JsProperty
        boolean isInsensitive();

        @JsProperty
        boolean isIsBackward();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setContains(boolean contains);

        @JsProperty
        void setInsensitive(boolean insensitive);

        @JsProperty
        void setIsBackward(boolean isBackward);

        @JsProperty
        void setSeekValue(String seekValue);

        @JsProperty
        void setSourceId(SeekRowRequest.ToObjectReturnType.SourceIdFieldType sourceId);

        @JsProperty
        void setStartingRow(String startingRow);

        @JsProperty
        void setValueType(String valueType);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                static SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static SeekRowRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.TicketFieldType ticket);
        }

        @JsOverlay
        static SeekRowRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        String getSeekValue();

        @JsProperty
        SeekRowRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        String getStartingRow();

        @JsProperty
        String getValueType();

        @JsProperty
        boolean isContains();

        @JsProperty
        boolean isInsensitive();

        @JsProperty
        boolean isIsBackward();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setContains(boolean contains);

        @JsProperty
        void setInsensitive(boolean insensitive);

        @JsProperty
        void setIsBackward(boolean isBackward);

        @JsProperty
        void setSeekValue(String seekValue);

        @JsProperty
        void setSourceId(SeekRowRequest.ToObjectReturnType0.SourceIdFieldType sourceId);

        @JsProperty
        void setStartingRow(String startingRow);

        @JsProperty
        void setValueType(String valueType);
    }

    public static native SeekRowRequest deserializeBinary(Uint8Array bytes);

    public static native SeekRowRequest deserializeBinaryFromReader(
            SeekRowRequest message, Object reader);

    public static native void serializeBinaryToWriter(SeekRowRequest message, Object writer);

    public static native SeekRowRequest.ToObjectReturnType toObject(
            boolean includeInstance, SeekRowRequest msg);

    public native void clearSourceId();

    public native String getColumnName();

    public native boolean getContains();

    public native boolean getInsensitive();

    public native boolean getIsBackward();

    public native String getSeekValue();

    public native TableReference getSourceId();

    public native String getStartingRow();

    public native String getValueType();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native void setContains(boolean value);

    public native void setInsensitive(boolean value);

    public native void setIsBackward(boolean value);

    public native void setSeekValue(String value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native void setStartingRow(String value);

    public native void setValueType(String value);

    public native SeekRowRequest.ToObjectReturnType0 toObject();

    public native SeekRowRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

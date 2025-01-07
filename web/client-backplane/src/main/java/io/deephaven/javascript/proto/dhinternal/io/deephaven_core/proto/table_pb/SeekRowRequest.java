//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.SeekRowRequest",
        namespace = JsPackage.GLOBAL)
public class SeekRowRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SeekValueFieldType {
            @JsOverlay
            static SeekRowRequest.ToObjectReturnType.SeekValueFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDoubleValue();

            @JsProperty
            String getLongValue();

            @JsProperty
            String getNanoTimeValue();

            @JsProperty
            String getStringValue();

            @JsProperty
            boolean isBoolValue();

            @JsProperty
            void setBoolValue(boolean boolValue);

            @JsProperty
            void setDoubleValue(double doubleValue);

            @JsProperty
            void setLongValue(String longValue);

            @JsProperty
            void setNanoTimeValue(String nanoTimeValue);

            @JsProperty
            void setStringValue(String stringValue);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static SeekRowRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType of(Object o) {
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
            static SeekRowRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            SeekRowRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(SeekRowRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<SeekRowRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<SeekRowRequest.ToObjectReturnType.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static SeekRowRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        SeekRowRequest.ToObjectReturnType.SeekValueFieldType getSeekValue();

        @JsProperty
        SeekRowRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        String getStartingRow();

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
        void setSeekValue(SeekRowRequest.ToObjectReturnType.SeekValueFieldType seekValue);

        @JsProperty
        void setSourceId(SeekRowRequest.ToObjectReturnType.SourceIdFieldType sourceId);

        @JsProperty
        void setStartingRow(String startingRow);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SeekValueFieldType {
            @JsOverlay
            static SeekRowRequest.ToObjectReturnType0.SeekValueFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDoubleValue();

            @JsProperty
            String getLongValue();

            @JsProperty
            String getNanoTimeValue();

            @JsProperty
            String getStringValue();

            @JsProperty
            boolean isBoolValue();

            @JsProperty
            void setBoolValue(boolean boolValue);

            @JsProperty
            void setDoubleValue(double doubleValue);

            @JsProperty
            void setLongValue(String longValue);

            @JsProperty
            void setNanoTimeValue(String nanoTimeValue);

            @JsProperty
            void setStringValue(String stringValue);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType of(
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
            static SeekRowRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<SeekRowRequest.ToObjectReturnType0.SourceIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static SeekRowRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        SeekRowRequest.ToObjectReturnType0.SeekValueFieldType getSeekValue();

        @JsProperty
        SeekRowRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        String getStartingRow();

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
        void setSeekValue(SeekRowRequest.ToObjectReturnType0.SeekValueFieldType seekValue);

        @JsProperty
        void setSourceId(SeekRowRequest.ToObjectReturnType0.SourceIdFieldType sourceId);

        @JsProperty
        void setStartingRow(String startingRow);
    }

    public static native SeekRowRequest deserializeBinary(Uint8Array bytes);

    public static native SeekRowRequest deserializeBinaryFromReader(
            SeekRowRequest message, Object reader);

    public static native void serializeBinaryToWriter(SeekRowRequest message, Object writer);

    public static native SeekRowRequest.ToObjectReturnType toObject(
            boolean includeInstance, SeekRowRequest msg);

    public native void clearSeekValue();

    public native void clearSourceId();

    public native String getColumnName();

    public native boolean getContains();

    public native boolean getInsensitive();

    public native boolean getIsBackward();

    public native Literal getSeekValue();

    public native Ticket getSourceId();

    public native String getStartingRow();

    public native boolean hasSeekValue();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native void setContains(boolean value);

    public native void setInsensitive(boolean value);

    public native void setIsBackward(boolean value);

    public native void setSeekValue();

    public native void setSeekValue(Literal value);

    public native void setSourceId();

    public native void setSourceId(Ticket value);

    public native void setStartingRow(String value);

    public native SeekRowRequest.ToObjectReturnType0 toObject();

    public native SeekRowRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

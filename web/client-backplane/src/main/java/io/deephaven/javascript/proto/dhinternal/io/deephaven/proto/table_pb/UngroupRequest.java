package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UngroupRequest",
        namespace = JsPackage.GLOBAL)
public class UngroupRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static UngroupRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(Object o) {
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
            static UngroupRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UngroupRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(UngroupRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<UngroupRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<UngroupRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static UngroupRequest.ToObjectReturnType.SourceIdFieldType create() {
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
        static UngroupRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToUngroupList();

        @JsProperty
        UngroupRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        UngroupRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isNullFill();

        @JsProperty
        void setColumnsToUngroupList(JsArray<String> columnsToUngroupList);

        @JsOverlay
        default void setColumnsToUngroupList(String[] columnsToUngroupList) {
            setColumnsToUngroupList(Js.<JsArray<String>>uncheckedCast(columnsToUngroupList));
        }

        @JsProperty
        void setNullFill(boolean nullFill);

        @JsProperty
        void setResultId(UngroupRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(UngroupRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static UngroupRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static UngroupRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UngroupRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    UngroupRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<UngroupRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<UngroupRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static UngroupRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
        static UngroupRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToUngroupList();

        @JsProperty
        UngroupRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        UngroupRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isNullFill();

        @JsProperty
        void setColumnsToUngroupList(JsArray<String> columnsToUngroupList);

        @JsOverlay
        default void setColumnsToUngroupList(String[] columnsToUngroupList) {
            setColumnsToUngroupList(Js.<JsArray<String>>uncheckedCast(columnsToUngroupList));
        }

        @JsProperty
        void setNullFill(boolean nullFill);

        @JsProperty
        void setResultId(UngroupRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(UngroupRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native UngroupRequest deserializeBinary(Uint8Array bytes);

    public static native UngroupRequest deserializeBinaryFromReader(
            UngroupRequest message, Object reader);

    public static native void serializeBinaryToWriter(UngroupRequest message, Object writer);

    public static native UngroupRequest.ToObjectReturnType toObject(
            boolean includeInstance, UngroupRequest msg);

    public native String addColumnsToUngroup(String value, double index);

    public native String addColumnsToUngroup(String value);

    public native void clearColumnsToUngroupList();

    public native void clearResultId();

    public native void clearSourceId();

    public native JsArray<String> getColumnsToUngroupList();

    public native boolean getNullFill();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setColumnsToUngroupList(JsArray<String> value);

    @JsOverlay
    public final void setColumnsToUngroupList(String[] value) {
        setColumnsToUngroupList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setNullFill(boolean value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native UngroupRequest.ToObjectReturnType0 toObject();

    public native UngroupRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

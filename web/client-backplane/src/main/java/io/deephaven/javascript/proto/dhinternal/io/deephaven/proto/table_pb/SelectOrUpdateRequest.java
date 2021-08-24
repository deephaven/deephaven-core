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
    name = "dhinternal.io.deephaven.proto.table_pb.SelectOrUpdateRequest",
    namespace = JsPackage.GLOBAL)
public class SelectOrUpdateRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static SelectOrUpdateRequest.ToObjectReturnType.SourceIdFieldType create() {
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
        static SelectOrUpdateRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnSpecsList();

        @JsProperty
        SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        SelectOrUpdateRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        void setColumnSpecsList(JsArray<String> columnSpecsList);

        @JsOverlay
        default void setColumnSpecsList(String[] columnSpecsList) {
            setColumnSpecsList(Js.<JsArray<String>>uncheckedCast(columnSpecsList));
        }

        @JsProperty
        void setResultId(SelectOrUpdateRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(SelectOrUpdateRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static SelectOrUpdateRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
        static SelectOrUpdateRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnSpecsList();

        @JsProperty
        SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        SelectOrUpdateRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        void setColumnSpecsList(JsArray<String> columnSpecsList);

        @JsOverlay
        default void setColumnSpecsList(String[] columnSpecsList) {
            setColumnSpecsList(Js.<JsArray<String>>uncheckedCast(columnSpecsList));
        }

        @JsProperty
        void setResultId(SelectOrUpdateRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(SelectOrUpdateRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native SelectOrUpdateRequest deserializeBinary(Uint8Array bytes);

    public static native SelectOrUpdateRequest deserializeBinaryFromReader(
        SelectOrUpdateRequest message, Object reader);

    public static native void serializeBinaryToWriter(SelectOrUpdateRequest message, Object writer);

    public static native SelectOrUpdateRequest.ToObjectReturnType toObject(
        boolean includeInstance, SelectOrUpdateRequest msg);

    public native String addColumnSpecs(String value, double index);

    public native String addColumnSpecs(String value);

    public native void clearColumnSpecsList();

    public native void clearResultId();

    public native void clearSourceId();

    public native JsArray<String> getColumnSpecsList();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setColumnSpecsList(JsArray<String> value);

    @JsOverlay
    public final void setColumnSpecsList(String[] value) {
        setColumnSpecsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native SelectOrUpdateRequest.ToObjectReturnType0 toObject();

    public native SelectOrUpdateRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

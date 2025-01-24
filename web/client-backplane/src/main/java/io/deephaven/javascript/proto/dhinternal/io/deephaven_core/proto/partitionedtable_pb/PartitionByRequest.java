//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb;

import elemental2.core.JsArray;
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
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionByRequest",
        namespace = JsPackage.GLOBAL)
public class PartitionByRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static PartitionByRequest.ToObjectReturnType.TableIdFieldType.GetTicketUnionType of(
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
            static PartitionByRequest.ToObjectReturnType.TableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            PartitionByRequest.ToObjectReturnType.TableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    PartitionByRequest.ToObjectReturnType.TableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<PartitionByRequest.ToObjectReturnType.TableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<PartitionByRequest.ToObjectReturnType.TableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static PartitionByRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getKeyColumnNamesList();

        @JsProperty
        Object getResultId();

        @JsProperty
        PartitionByRequest.ToObjectReturnType.TableIdFieldType getTableId();

        @JsProperty
        boolean isDropKeys();

        @JsProperty
        void setDropKeys(boolean dropKeys);

        @JsProperty
        void setKeyColumnNamesList(JsArray<String> keyColumnNamesList);

        @JsOverlay
        default void setKeyColumnNamesList(String[] keyColumnNamesList) {
            setKeyColumnNamesList(Js.<JsArray<String>>uncheckedCast(keyColumnNamesList));
        }

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setTableId(PartitionByRequest.ToObjectReturnType.TableIdFieldType tableId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static PartitionByRequest.ToObjectReturnType0.TableIdFieldType.GetTicketUnionType of(
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
            static PartitionByRequest.ToObjectReturnType0.TableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            PartitionByRequest.ToObjectReturnType0.TableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    PartitionByRequest.ToObjectReturnType0.TableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<PartitionByRequest.ToObjectReturnType0.TableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<PartitionByRequest.ToObjectReturnType0.TableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static PartitionByRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getKeyColumnNamesList();

        @JsProperty
        Object getResultId();

        @JsProperty
        PartitionByRequest.ToObjectReturnType0.TableIdFieldType getTableId();

        @JsProperty
        boolean isDropKeys();

        @JsProperty
        void setDropKeys(boolean dropKeys);

        @JsProperty
        void setKeyColumnNamesList(JsArray<String> keyColumnNamesList);

        @JsOverlay
        default void setKeyColumnNamesList(String[] keyColumnNamesList) {
            setKeyColumnNamesList(Js.<JsArray<String>>uncheckedCast(keyColumnNamesList));
        }

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setTableId(PartitionByRequest.ToObjectReturnType0.TableIdFieldType tableId);
    }

    public static native PartitionByRequest deserializeBinary(Uint8Array bytes);

    public static native PartitionByRequest deserializeBinaryFromReader(
            PartitionByRequest message, Object reader);

    public static native void serializeBinaryToWriter(PartitionByRequest message, Object writer);

    public static native PartitionByRequest.ToObjectReturnType toObject(
            boolean includeInstance, PartitionByRequest msg);

    public native String addKeyColumnNames(String value, double index);

    public native String addKeyColumnNames(String value);

    public native void clearKeyColumnNamesList();

    public native void clearResultId();

    public native void clearTableId();

    public native boolean getDropKeys();

    public native JsArray<String> getKeyColumnNamesList();

    public native Ticket getResultId();

    public native Ticket getTableId();

    public native boolean hasResultId();

    public native boolean hasTableId();

    public native Uint8Array serializeBinary();

    public native void setDropKeys(boolean value);

    public native void setKeyColumnNamesList(JsArray<String> value);

    @JsOverlay
    public final void setKeyColumnNamesList(String[] value) {
        setKeyColumnNamesList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setTableId();

    public native void setTableId(Ticket value);

    public native PartitionByRequest.ToObjectReturnType0 toObject();

    public native PartitionByRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

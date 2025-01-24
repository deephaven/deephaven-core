//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

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
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableViewKeyTableDescriptor",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableViewKeyTableDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface KeyTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getKeyTableActionColumn();

        @JsProperty
        HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType getKeyTableId();

        @JsProperty
        void setKeyTableActionColumn(String keyTableActionColumn);

        @JsProperty
        void setKeyTableId(
                HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType.KeyTableIdFieldType keyTableId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface KeyTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getKeyTableActionColumn();

        @JsProperty
        HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType getKeyTableId();

        @JsProperty
        void setKeyTableActionColumn(String keyTableActionColumn);

        @JsProperty
        void setKeyTableId(
                HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0.KeyTableIdFieldType keyTableId);
    }

    public static native HierarchicalTableViewKeyTableDescriptor deserializeBinary(Uint8Array bytes);

    public static native HierarchicalTableViewKeyTableDescriptor deserializeBinaryFromReader(
            HierarchicalTableViewKeyTableDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(
            HierarchicalTableViewKeyTableDescriptor message, Object writer);

    public static native HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, HierarchicalTableViewKeyTableDescriptor msg);

    public native void clearKeyTableActionColumn();

    public native void clearKeyTableId();

    public native String getKeyTableActionColumn();

    public native Ticket getKeyTableId();

    public native boolean hasKeyTableActionColumn();

    public native boolean hasKeyTableId();

    public native Uint8Array serializeBinary();

    public native void setKeyTableActionColumn(String value);

    public native void setKeyTableId();

    public native void setKeyTableId(Ticket value);

    public native HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0 toObject();

    public native HierarchicalTableViewKeyTableDescriptor.ToObjectReturnType0 toObject(
            boolean includeInstance);
}

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
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.TreeRequest",
        namespace = JsPackage.GLOBAL)
public class TreeRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultTreeTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType.GetTicketUnionType of(
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
            static TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static TreeRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getIdentifierColumn();

        @JsProperty
        String getParentIdentifierColumn();

        @JsProperty
        TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType getResultTreeTableId();

        @JsProperty
        Object getSourceTableId();

        @JsProperty
        boolean isPromoteOrphans();

        @JsProperty
        void setIdentifierColumn(String identifierColumn);

        @JsProperty
        void setParentIdentifierColumn(String parentIdentifierColumn);

        @JsProperty
        void setPromoteOrphans(boolean promoteOrphans);

        @JsProperty
        void setResultTreeTableId(
                TreeRequest.ToObjectReturnType.ResultTreeTableIdFieldType resultTreeTableId);

        @JsProperty
        void setSourceTableId(Object sourceTableId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultTreeTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType.GetTicketUnionType of(
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
            static TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static TreeRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getIdentifierColumn();

        @JsProperty
        String getParentIdentifierColumn();

        @JsProperty
        TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType getResultTreeTableId();

        @JsProperty
        Object getSourceTableId();

        @JsProperty
        boolean isPromoteOrphans();

        @JsProperty
        void setIdentifierColumn(String identifierColumn);

        @JsProperty
        void setParentIdentifierColumn(String parentIdentifierColumn);

        @JsProperty
        void setPromoteOrphans(boolean promoteOrphans);

        @JsProperty
        void setResultTreeTableId(
                TreeRequest.ToObjectReturnType0.ResultTreeTableIdFieldType resultTreeTableId);

        @JsProperty
        void setSourceTableId(Object sourceTableId);
    }

    public static native TreeRequest deserializeBinary(Uint8Array bytes);

    public static native TreeRequest deserializeBinaryFromReader(TreeRequest message, Object reader);

    public static native void serializeBinaryToWriter(TreeRequest message, Object writer);

    public static native TreeRequest.ToObjectReturnType toObject(
            boolean includeInstance, TreeRequest msg);

    public native void clearResultTreeTableId();

    public native void clearSourceTableId();

    public native String getIdentifierColumn();

    public native String getParentIdentifierColumn();

    public native boolean getPromoteOrphans();

    public native Ticket getResultTreeTableId();

    public native Ticket getSourceTableId();

    public native boolean hasResultTreeTableId();

    public native boolean hasSourceTableId();

    public native Uint8Array serializeBinary();

    public native void setIdentifierColumn(String value);

    public native void setParentIdentifierColumn(String value);

    public native void setPromoteOrphans(boolean value);

    public native void setResultTreeTableId();

    public native void setResultTreeTableId(Ticket value);

    public native void setSourceTableId();

    public native void setSourceTableId(Ticket value);

    public native TreeRequest.ToObjectReturnType0 toObject();

    public native TreeRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

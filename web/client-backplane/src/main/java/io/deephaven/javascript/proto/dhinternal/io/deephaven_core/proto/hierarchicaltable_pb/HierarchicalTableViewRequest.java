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
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableViewRequest",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableViewRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ExpansionsFieldType {
            @JsOverlay
            static HierarchicalTableViewRequest.ToObjectReturnType.ExpansionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getKeyTableActionColumn();

            @JsProperty
            Object getKeyTableId();

            @JsProperty
            void setKeyTableActionColumn(String keyTableActionColumn);

            @JsProperty
            void setKeyTableId(Object keyTableId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultViewIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static HierarchicalTableViewRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getExistingViewId();

        @JsProperty
        HierarchicalTableViewRequest.ToObjectReturnType.ExpansionsFieldType getExpansions();

        @JsProperty
        Object getHierarchicalTableId();

        @JsProperty
        HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType getResultViewId();

        @JsProperty
        void setExistingViewId(Object existingViewId);

        @JsProperty
        void setExpansions(
                HierarchicalTableViewRequest.ToObjectReturnType.ExpansionsFieldType expansions);

        @JsProperty
        void setHierarchicalTableId(Object hierarchicalTableId);

        @JsProperty
        void setResultViewId(
                HierarchicalTableViewRequest.ToObjectReturnType.ResultViewIdFieldType resultViewId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ExpansionsFieldType {
            @JsOverlay
            static HierarchicalTableViewRequest.ToObjectReturnType0.ExpansionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getKeyTableActionColumn();

            @JsProperty
            Object getKeyTableId();

            @JsProperty
            void setKeyTableActionColumn(String keyTableActionColumn);

            @JsProperty
            void setKeyTableId(Object keyTableId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultViewIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static HierarchicalTableViewRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getExistingViewId();

        @JsProperty
        HierarchicalTableViewRequest.ToObjectReturnType0.ExpansionsFieldType getExpansions();

        @JsProperty
        Object getHierarchicalTableId();

        @JsProperty
        HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType getResultViewId();

        @JsProperty
        void setExistingViewId(Object existingViewId);

        @JsProperty
        void setExpansions(
                HierarchicalTableViewRequest.ToObjectReturnType0.ExpansionsFieldType expansions);

        @JsProperty
        void setHierarchicalTableId(Object hierarchicalTableId);

        @JsProperty
        void setResultViewId(
                HierarchicalTableViewRequest.ToObjectReturnType0.ResultViewIdFieldType resultViewId);
    }

    public static native HierarchicalTableViewRequest deserializeBinary(Uint8Array bytes);

    public static native HierarchicalTableViewRequest deserializeBinaryFromReader(
            HierarchicalTableViewRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            HierarchicalTableViewRequest message, Object writer);

    public static native HierarchicalTableViewRequest.ToObjectReturnType toObject(
            boolean includeInstance, HierarchicalTableViewRequest msg);

    public native void clearExistingViewId();

    public native void clearExpansions();

    public native void clearHierarchicalTableId();

    public native void clearResultViewId();

    public native Ticket getExistingViewId();

    public native HierarchicalTableViewKeyTableDescriptor getExpansions();

    public native Ticket getHierarchicalTableId();

    public native Ticket getResultViewId();

    public native int getTargetCase();

    public native boolean hasExistingViewId();

    public native boolean hasExpansions();

    public native boolean hasHierarchicalTableId();

    public native boolean hasResultViewId();

    public native Uint8Array serializeBinary();

    public native void setExistingViewId();

    public native void setExistingViewId(Ticket value);

    public native void setExpansions();

    public native void setExpansions(HierarchicalTableViewKeyTableDescriptor value);

    public native void setHierarchicalTableId();

    public native void setHierarchicalTableId(Ticket value);

    public native void setResultViewId();

    public native void setResultViewId(Ticket value);

    public native HierarchicalTableViewRequest.ToObjectReturnType0 toObject();

    public native HierarchicalTableViewRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}

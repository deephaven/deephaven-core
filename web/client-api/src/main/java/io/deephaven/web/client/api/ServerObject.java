//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.api.widget.JsWidgetExportedObject;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

/**
 * Indicates that this object is a local representation of an object that exists on the server. Similar to HasLifecycle,
 * but not quite the same - whereas HasLifecycle is entirely internal and provides and provides hooks for reconnect
 * logic, this exists to get a typed ticket reference to an object on the server, to pass that same ticket back to the
 * server again.
 */
public interface ServerObject {
    @JsIgnore
    TypedTicket typedTicket();

    /**
     * Note that we don't explicitly use this as a union type, but sort of as a way to pretend that ServerObject is a
     * sealed type with this generated set of subtypes.
     */
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    @TsUnion
    interface Union {
        @TsUnionMember
        @JsOverlay
        default JsTable asTable() {
            return (JsTable) this;
        }

        @TsUnionMember
        @JsOverlay
        default JsWidget asWidget() {
            return (JsWidget) this;
        }

        @TsUnionMember
        @JsOverlay
        default JsWidgetExportedObject asWidgetExportedObject() {
            return (JsWidgetExportedObject) this;
        }

        @TsUnionMember
        @JsOverlay
        default JsPartitionedTable asPartitionedTable() {
            return (JsPartitionedTable) this;
        }

        @TsUnionMember
        @JsOverlay
        default JsTotalsTable asTotalsTable() {
            return (JsTotalsTable) this;
        }

        @TsUnionMember
        @JsOverlay
        default JsTreeTable asTreeTable() {
            return (JsTreeTable) this;
        }

        @JsOverlay
        default ServerObject asServerObject() {
            return (ServerObject) this;
        }
    }
}

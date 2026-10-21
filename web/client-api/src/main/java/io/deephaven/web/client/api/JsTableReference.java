//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Any client object that refers to a table on the server, and so can be the other operand of a table operation: a
 * {@link JsTable}, a {@link JsTotalsTable}, or any {@link JsTableOperations} such as a {@link JsPendingTable} or
 * {@link JsImmutableTable}. Operations only need the table's ticket; they never retain or read the operand.
 */
@TsUnion(anonymous = false, name = "TableReference", namespace = "dh")
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface JsTableReference {
    @JsOverlay
    static JsTableReference of(ServerObject object) {
        return Js.cast(object);
    }

    @TsUnionMember
    @JsOverlay
    default JsTable asTable() {
        return Js.cast(this);
    }

    @TsUnionMember
    @JsOverlay
    default JsTotalsTable asTotalsTable() {
        return Js.cast(this);
    }

    @TsUnionMember
    @JsOverlay
    default JsTableOperations asTableOperations() {
        return Js.cast(this);
    }

    /**
     * Internal: every member of this union is a {@link ServerObject}.
     */
    @JsOverlay
    default ServerObject asServerObject() {
        return (ServerObject) this;
    }

    @JsOverlay
    default TypedTicket typedTicket() {
        return asServerObject().typedTicket();
    }

    @JsOverlay
    default WorkerConnection getConnection() {
        return asServerObject().getConnection();
    }
}

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
 * {@link JsTable}, a {@link JsTotalsTable}, a {@link JsPendingTable} or a {@link JsResolvedTable}. These are the
 * table-shaped members of the union of server objects. The operand must be on the same connection as the table operated
 * on: the operations of {@link JsTableOperations} and the joins of a mutable {@code Table} throw if it is not, and the
 * other methods taking one reject. A {@link JsTable} contributes its current state, including filters or sorts
 * requested but not yet applied.
 * <p>
 * Every method that takes a table reference holds it until its result settles, so the caller may close it as soon as
 * the method has been called: the operations of {@link JsTableOperations}, the joins and {@code snapshot} of a mutable
 * {@code Table}, {@code mergeTables}, the {@code InputTable} add and delete methods, {@code shareObject} and
 * {@code bindTableToVariable}. {@code Widget.sendMessage} is the exception: it does not hold its references.
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
    default JsPendingTable<?> asPendingTable() {
        return Js.cast(this);
    }

    @TsUnionMember
    @JsOverlay
    default JsResolvedTable asResolvedTable() {
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

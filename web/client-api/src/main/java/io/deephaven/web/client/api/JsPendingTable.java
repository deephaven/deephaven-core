//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsDefault;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.fu.PromiseLike;
import jsinterop.annotations.JsMethod;

/**
 * Represents a table being created on the server that more operations can be called on asynchronously. Supports all the
 * methods of {@link JsTableOperations}, which will each in turn return another PendingTable.
 * <p>
 * This interface is "Thenable" / "PromiseLike", it can be awaited or have methods chained to it like a promise to
 * resolve into a table with metadata that can have data fetched from it. What awaiting gives depends on where the
 * pending table came from: an operation on {@link JsTableOperations} yields a {@link JsResolvedTable}, while the APIs
 * that have always produced a {@link JsTable}, such as fetching a named table, yield a {@link JsTable}.
 * {@link #resolve()} always yields a {@link JsResolvedTable}. A pending table produces one table: await it or call
 * {@code resolve()} on it, not both; either may be repeated and gives the same table. The resulting table must be
 * closed to indicate that it will no longer be used and server resources can be freed. A future version of this API
 * could provide "liveness scopes" to claim/release batches of tables at a time automatically.
 * <p>
 * A pending table may only be used in the task that created it, before control returns to the event loop: awaited,
 * {@code then()} or {@code resolve()} called on it, chained on, passed as an operand, or passed to
 * {@code Promise.resolve} or {@code Promise.all}. What lasts is the result: the table an await produces, which is
 * retained until {@code close()} is called on it and has the same operations, or the promise from
 * {@code Promise.resolve}. A pending table that is not awaited is freed automatically as soon as possible: once its
 * response arrives and nothing chained on it still needs it. Used in a later task, awaited or not, the operation
 * chained on it, or the {@code await}, is rejected, and a warning names where the pending table was created; a use the
 * client cannot yet tell is in a later task behaves as it would by timing alone. In particular, awaiting two pending
 * tables one after the other, {@code await a; await b;}, rejects {@code b}: await both at once with
 * {@code Promise.all([a, b])}, or each as it is created. This holds for a fetch as much as for an operation.
 *
 * @param <T> the table awaiting this gives. In TypeScript it defaults to {@code ResolvedTable}, what every operation
 *        produces. A fetched table resolves to a {@code Table} instead: narrow what {@code getObject} returns with a
 *        property test such as {@code "where" in result}, or with a cast to {@code PendingTable<Table>}; TypeScript
 *        rejects a cast to a bare {@code PendingTable}.
 */
@TsName(namespace = "dh", name = "PendingTable")
@TsInterface
public interface JsPendingTable<@TsDefault(JsResolvedTable.class) T extends JsITable>
        extends JsTableOperations, PromiseLike<T> {
    /**
     * Releases everything this pending table produced, whichever stage it is at. If the operation is still pending, it
     * is canceled: the promise from {@link #resolve()} rejects with "Operation canceled" and the ticket is released. If
     * the table was already delivered, it is closed, for every awaiter. Operations already chained on it that have not
     * yet produced their table reject with "Source table failed: " and the cancel message; operations chained on it
     * afterward are never sent, and reject with "Cannot use source table: " and the cancel message. The server cancels
     * work it has not started, but work it has already begun runs to completion there, so callers that cancel often
     * should debounce.
     */
    @JsMethod
    void cancel();
}

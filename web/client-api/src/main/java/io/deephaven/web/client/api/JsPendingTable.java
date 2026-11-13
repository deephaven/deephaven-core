//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsDefault;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.promise.Promise;
import io.deephaven.web.client.fu.PromiseLike;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;

/**
 * A table being created on the server, on which more operations can be chained before it exists. Supports all the
 * methods of {@link JsTableOperations}, each of which returns another pending table.
 * <p>
 * A pending table is a thenable: it can be awaited, or given callbacks with {@code then}, and resolves to a table whose
 * data can be read. What it resolves to depends on where it came from:
 * <ul>
 * <li>an operation on {@link JsTableOperations}, or the {@link JsTable#operations()} of a {@link JsTable} — a
 * {@link JsResolvedTable}</li>
 * <li>an API that has always produced a {@link JsTable}, such as fetching a named table — a {@link JsTable}</li>
 * </ul>
 * {@link #resolve()} always gives a {@link JsResolvedTable}. For an operation, awaiting and resolving give the same
 * table. A fetched pending table produces only one of the two, so await it or resolve it, not both. Either may be
 * repeated within the task that created it, and gives the same table each time.
 * <p>
 * A pending table has no {@code catch} or {@code finally}. For those, wrap it with {@code Promise.resolve(...)} in the
 * task that created it.
 * <p>
 * A pending table has no owner. Whoever awaits or resolves it must close the resulting table, so the server can free
 * its resources. {@link JsTableOperations} describes who owns what.
 * <p>
 * A pending table may only be used in the task that created it, before control returns to the event loop: awaited,
 * {@code then} or {@link #resolve()} called on it, chained on, passed as an operand, or passed to
 * {@code Promise.resolve} or {@code Promise.all}. What lasts is the result: the table an await produces, which is
 * retained until it is closed and has the same operations, or the promise from {@code Promise.resolve}. A pending table
 * that is not awaited is freed automatically as soon as possible: once its response arrives and nothing chained on it
 * still needs it. Used in a later task, awaited or not, the operation chained on it, or the {@code await}, is rejected,
 * and a warning names where the pending table was created. A use the client cannot yet tell is in a later task behaves
 * as it would by timing alone. In particular, awaiting two pending tables one after the other,
 * {@code await a; await b;}, rejects {@code b}: await both at once with {@code Promise.all([a, b])}, or each as it is
 * created. This holds for a fetch as much as for an operation. A {@link JsTable#operations()} is often ready at once,
 * so awaiting it may not leave the task and a second await can happen to work. Do not rely on that, since a change
 * still being applied to the table makes it wait like any other. If one of the tables passed to {@code Promise.all}
 * fails, it rejects without the others, which still arrive and must still be closed. When any of them can fail, use
 * {@code Promise.allSettled} and close those that arrived.
 *
 * @param <T> the table awaiting this gives. In TypeScript it defaults to {@link JsResolvedTable}, what every operation
 *        produces. A fetched table resolves to a {@link JsTable} instead: narrow what
 *        {@link io.deephaven.web.client.ide.IdeConnection#getObject IdeConnection.getObject} or
 *        {@link io.deephaven.web.client.ide.IdeSession#getObject IdeSession.getObject} returns with a property test
 *        such as {@code "where" in result}, or with a cast to <code>PendingTable&lt;Table&gt;</code>. TypeScript
 *        rejects a cast to a bare {@code PendingTable}.
 */
@TsName(namespace = "dh", name = "PendingTable")
@TsInterface
public interface JsPendingTable<@TsDefault(JsResolvedTable.class) T> extends JsTableOperations, PromiseLike<T> {
    /**
     * Releases everything this pending table produced, whichever stage it is at. If the operation is still pending, it
     * is canceled: the promise from {@link #resolve()} rejects with "Operation canceled" and the ticket is released. If
     * the table was already delivered, it is closed, for every awaiter. Operations already chained on it that have not
     * yet produced their table reject with "Source table failed: " and the cancel message. Operations chained on it
     * afterward are never sent, and reject with "Cannot use source table: " and the cancel message. The server cancels
     * work it has not started, but work it has already begun runs to completion there, so callers that cancel often
     * should debounce. Canceling the {@link JsTable#operations()} of a {@link JsTable} works the same way, except that
     * the {@link JsTable} and its server-side table are left as they are. A call that already named this table and has
     * an effect of its own, such as {@link io.deephaven.web.client.api.input.JsInputTable#addTable
     * InputTable.addTable}, {@link io.deephaven.web.client.ide.IdeConnection#shareObject IdeConnection.shareObject} or
     * {@link io.deephaven.web.client.ide.IdeSession#shareObject IdeSession.shareObject}, is not withdrawn.
     */
    @JsMethod
    void cancel();

    /**
     * The table this pending table produces, as a {@link JsResolvedTable} whose data can be read and on which more
     * operations can be chained, whatever awaiting it gives. This holds for the {@link JsTable#operations()} of a
     * {@link JsTable} too. Repeated calls give the same promise and the same table. Resolving does not change who owns
     * the table. Use {@link JsResolvedTable#copy()} for an instance with its own lifetime. A pending table that was
     * canceled, or is resolved after the task that created it, rejects instead, as does a fetched one that was already
     * awaited.
     *
     * @param options optionally, a {@code signal} that cancels this pending table when aborted
     * @return a promise of the resolved table
     */
    @JsMethod
    Promise<JsResolvedTable> resolve(@JsOptional @JsNullable @TsTypeRef(ResolveOptions.class) Object options);

    /**
     * As {@link #resolve(Object)} with no options.
     */
    @JsIgnore
    default Promise<JsResolvedTable> resolve() {
        return resolve(null);
    }
}

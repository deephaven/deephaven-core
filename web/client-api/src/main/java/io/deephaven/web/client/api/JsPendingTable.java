//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsDefault;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.ReadonlyArray;
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
 * On the other hand, {@link #resolve(Object)} always gives a {@link JsResolvedTable}. Either await or
 * {@link #resolve(Object) resolve} a pending table, but not both. Repeated usage of either yields the same instance.
 * <p>
 * A pending table has no {@code catch} or {@code finally}. For those, wrap it with {@code Promise.resolve(...)} in the
 * task that created it.
 * <p>
 * A pending table has no owner. Whoever awaits or resolves it must close the resulting table, so the server can free
 * its resources. {@link JsTableOperations} describes who owns what.
 * <p>
 * A pending table is only valid in the task that created it. Any use after the task is an error, and usually rejects -
 * the one exception is {@link #cancel()}. It cannot be awaited, {@link #resolve(Object) resolved}, {@code then()}
 * invoked or otherwise used as a promise, passed as an argument to another operation, or have operations called on it.
 * <p>
 *
 * When dealing with multiple pending tables, either individually use {@code Promise.resolve} or
 * {@link #resolve(Object)} and await those promises, or use {@link DhNamespace#resolveAll(ReadonlyArray, Object)} to
 * automatically await their successes, and clean them all up if any one fails.
 *
 * @param <T> the type of table awaiting this resolves to. Use {@link #resolve(Object)} to get {@link JsResolvedTable}.
 */
@TsName(namespace = "dh", name = "PendingTable")
@TsInterface
public interface JsPendingTable<@TsDefault(JsResolvedTable.class) T> extends JsTableOperations, PromiseLike<T> {
    /**
     * Releases everything this pending table produced, whichever stage it is at. If the operation is still pending, it
     * is canceled: the promise from {@link #resolve(Object)} rejects with "Operation canceled" and the ticket is
     * released. If the table was already delivered, it is closed, for every awaiter. Operations already chained on it
     * that have not yet produced their table reject with "Source table failed: " and the cancel message. Operations
     * chained on it afterward are never sent, and reject with "Cannot use source table: " and the cancel message. The
     * server cancels work it has not started, but work it has already begun runs to completion there, so callers that
     * cancel often should debounce. Canceling the {@link JsTable#operations()} of a {@link JsTable} works the same way,
     * except that the {@link JsTable} and its server-side table are left as they are. A call that already named this
     * table and has an effect of its own, such as {@link io.deephaven.web.client.api.input.JsInputTable#addTable
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

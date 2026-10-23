//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

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
 * Represents a table being created on the server that more operations can be called on asynchronously. Supports all the
 * methods of {@link JsTableOperations}, which will each in turn return another PendingTable.
 * <p>
 * This interface is "Thenable" / "PromiseLike", it can be awaited or have methods chained to it like a promise to
 * resolve into a table with metadata that can have data fetched from it. What that table is depends on where the
 * pending table came from: an operation on {@link JsTableOperations} yields a {@link JsResolvedTable}, while the APIs
 * that have always produced a {@link JsTable}, such as fetching a named table, yield a {@link JsTable}. Either way the
 * resulting table must be closed to indicate that it will no longer be used and server resources can be freed. A future
 * version of this API could provide "liveness scopes" to claim/release batches of tables at a time automatically.
 * <p>
 * A pending table from an operation that is not awaited only lasts long enough for methods to be synchronously called
 * on it, then is freed automatically as soon as possible. Any instance that is {@code await}ed will be retained until
 * {@code close()} is called on it to free it. To optionally chain more calls to an instance in a later event loop, it
 * must be retained first. A pending table from a fetch is retained from the start, as the promise it replaces was.
 *
 * @param <T> the table this resolves to
 */
@TsName(namespace = "dh", name = "PendingTable")
@TsInterface
public interface JsPendingTable<T extends JsITable> extends JsTableOperations, PromiseLike<T> {
    /**
     * The promise this pending table stands for: the same thing as awaiting it, with the resulting table's type spelled
     * out. Retains the table. Awaiting or resolving the same pending table more than once yields the same promise and
     * the same table, so closing it closes it for every awaiter; {@code copy()} on the result is how to get a second,
     * independently closeable owner.
     *
     * @param options optionally, an {@code AbortSignal} that cancels this pending table when aborted
     * @return a promise of the retained table
     */
    @Override
    @JsMethod
    Promise<T> resolve(@JsOptional @JsNullable @TsTypeRef(ResolveOptions.class) Object options);

    @Override
    @JsIgnore
    default Promise<T> resolve() {
        return resolve(null);
    }

    /**
     * Releases everything this pending table produced, whichever stage it is at. If the operation is still pending, it
     * is canceled: the promise from {@link #resolve()} rejects with "Operation canceled" and the ticket is released. If
     * the table was already delivered, it is closed, for every awaiter. Operations chained on it afterward throw. The
     * server cancels work it has not started, but work it has already begun runs to completion there, so callers that
     * cancel often should debounce.
     */
    // also happens to be a workaround for a javadoc -> ts bug
    @JsMethod
    void cancel();
}

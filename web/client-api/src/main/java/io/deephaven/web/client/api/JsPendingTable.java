//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.promise.Promise;
import io.deephaven.web.client.fu.PromiseLike;
import jsinterop.annotations.JsMethod;

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
     * out. Retains the table.
     *
     * @return a promise of the retained table
     */
    @Override
    @JsMethod
    Promise<T> resolve();

    // workaround for a javadoc -> ts issue
    @JsMethod
    void foo();

    /**
     * Cancels the operation: the promise from {@link #resolve()} rejects with "Operation canceled", the table this
     * pending would have produced is released, and operations chained on it afterward throw. A table that was already
     * delivered is unaffected; close it instead. The server cancels work it has not started, but work it has already
     * begun runs to completion there, so callers that cancel often should debounce.
     */
    // also happens to be a workaround for a javadoc -> ts bug
    @JsMethod
    void cancel();
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.fu.PromiseLike;
import jsinterop.annotations.JsMethod;

/**
 * Represents a table being created on the server that more operations can be called on asynchronously. Supports all the
 * methods of {@link JsTableOperations}, which will each in turn return another PendingTable.
 * <p>
 * This interface is "Thenable" / "PromiseLike", it can be awaited or have methods chained to it like a promise to
 * resolve into an object with metadata that can have data fetched from it, and will result in a
 * {@link JsResolvedTable}. If it is awaited, the resulting table must be closed to indicate that it will no longer be
 * used and server resources can be freed. A future version of this API could provide "liveness scopes" to claim/release
 * batches of tables at a time automatically.
 * <p>
 * Any instance not awaited will only last long enough for methods to be synchronously called on it, then freed
 * automatically as soon as possible. Any instance that is {@code await}ed will be retained until {@code close()} is
 * called on it to free it. To optionally chain more calls to an instance in a later event loop, it must be retained
 * first.
 */
@TsName(namespace = "dh", name = "PendingTable")
@TsInterface
public interface JsPendingTable extends JsTableOperations, PromiseLike<JsResolvedTable> {
    // workaround for a javadoc -> ts issue
    @JsMethod
    void foo();
}

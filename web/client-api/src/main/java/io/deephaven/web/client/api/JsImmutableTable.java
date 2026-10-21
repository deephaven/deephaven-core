//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.promise.Promise;
import jsinterop.annotations.JsMethod;

/**
 * The table an awaited {@link JsPendingTable} resolves to: a retained {@link JsResolvedTable} whose data can be read,
 * plus the chainable {@link JsTableOperations}, each of which again yields a {@link JsPendingTable}. It has no
 * client-side filter or sort state of its own; every change is a new server-side table.
 * <p>
 * Instances are owned by the caller and must be {@link #close() closed}. {@link #copy()} produces another instance over
 * the same server-side table that can be closed independently.
 */
@TsName(namespace = "dh", name = "ImmutableTable")
@TsInterface
public interface JsImmutableTable extends JsResolvedTable, JsTableOperations {
    /**
     * A new, independently closeable instance of this table. Every table obtained through {@code resolve()} or by
     * awaiting a {@link JsPendingTable} is one the caller owns and must close; this keeps that rule uniform for tables
     * that are already resolved.
     */
    @Override
    default Promise<JsImmutableTable> resolve() {
        return copy();
    }

    /**
     * Returns another instance of this table, allowing either instance to be closed without affecting the other.
     *
     * @return a copy of this table, sharing resources on the server
     */
    @JsMethod
    Promise<JsImmutableTable> copy();
}

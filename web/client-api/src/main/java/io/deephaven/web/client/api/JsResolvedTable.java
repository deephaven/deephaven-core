//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.promise.Promise;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;

/**
 * The table an awaited {@link JsPendingTable} resolves to: a retained {@link JsITable} whose data can be read, plus the
 * chainable {@link JsTableOperations}, each of which again yields a {@link JsPendingTable}. It has no client-side
 * filter or sort state of its own; every change is a new server-side table.
 * <p>
 * Instances are owned by the caller and must be {@link #close() closed}. {@link #copy()} produces another instance over
 * the same server-side table that can be closed independently.
 */
@TsName(namespace = "dh", name = "ResolvedTable")
@TsInterface
public interface JsResolvedTable extends JsITable, JsTableOperations {
    /**
     * An independently closeable copy of this table, so that whatever {@code resolve()} hands out is the caller's to
     * close.
     */
    @Override
    @JsMethod
    default Promise<JsResolvedTable> resolve(@JsOptional @JsNullable @TsTypeRef(ResolveOptions.class) Object options) {
        // Nothing pending to cancel: the signal is irrelevant here
        return copy();
    }

    @Override
    @JsIgnore
    default Promise<JsResolvedTable> resolve() {
        return resolve(null);
    }

    /**
     * Returns another instance of this table, allowing either instance to be closed without affecting the other.
     *
     * @return a copy of this table, sharing resources on the server
     */
    @JsMethod
    Promise<JsResolvedTable> copy();
}

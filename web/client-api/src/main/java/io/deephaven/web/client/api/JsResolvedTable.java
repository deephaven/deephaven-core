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
 * Each instance belongs to whoever created it or was given it to own, and must be {@link #close() closed} by them.
 * {@link #copy()} produces another instance over the same server-side table that can be closed independently; see
 * {@link JsTableOperations} for who owns what.
 */
@TsName(namespace = "dh", name = "ResolvedTable")
@TsInterface
public interface JsResolvedTable extends JsITable, JsTableOperations {
    /**
     * This table itself, as {@link JsTableOperations#resolve(Object)} gives for any table operations. Resolving does
     * not change who owns it; use {@link #copy()} for an instance with its own lifetime.
     *
     * @throws IllegalStateException if this table is closed
     */
    @Override
    @JsMethod
    Promise<JsResolvedTable> resolve(@JsOptional @JsNullable @TsTypeRef(ResolveOptions.class) Object options);

    @Override
    @JsIgnore
    default Promise<JsResolvedTable> resolve() {
        return resolve(null);
    }

    /**
     * A new instance over the same server-side table with its own lifetime, available at once. The caller owns it, and
     * either instance can be closed without affecting the other.
     *
     * @return a new instance of this table, sharing resources on the server
     * @throws IllegalStateException if this table is closed
     */
    @JsMethod
    JsResolvedTable copy();

    /**
     * A new {@link JsTable} over the same server-side table, the inverse of {@link JsTable#operations()}. The
     * {@code Table} can be changed with {@code applyFilter}, {@code applySort} and the other legacy methods without
     * affecting this table, and the two are closed independently. Like other tables made by operations, it is not
     * revived if a reconnect needs a new session.
     *
     * @return a new mutable table, which the caller must close
     */
    @JsMethod
    JsTable createMutableTable();
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.tree.JsRollupConfig;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.tree.JsTreeTableConfig;
import jsinterop.annotations.JsMethod;

/**
 * The table an awaited {@link JsPendingTable} resolves to: a retained table whose data can be read, plus the chainable
 * {@link JsTableOperations}, each of which again yields a {@link JsPendingTable}. It has no client-side filter or sort
 * state of its own; every change is a new server-side table.
 * <p>
 * Each instance must be closed, with {@code close()}, by whoever is responsible for it: the code that awaited or
 * resolved the pending table it came from, called {@link #copy()}, or was handed it to own. {@link #copy()} produces
 * another instance over the same server-side table that can be closed independently; see {@link JsTableOperations} for
 * who owns what. Like other tables made by operations, it is not revived if a reconnect needs a new session.
 */
@TsName(namespace = "dh", name = "ResolvedTable")
@TsInterface
public interface JsResolvedTable extends JsITable, JsTableOperations {
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

    /**
     * Finds the next row, after {@code startingRow} and wrapping around, whose value in {@code column} matches
     * {@code seekValue}.
     *
     * @param options the value to find, and where and how to search for it
     * @return a promise of the position of the matching row, or -1 if no row matches
     */
    @JsMethod
    Promise<Double> seekRow(SeekRowOptions options);

    /**
     * A promise that will resolve to a new {@link JsTreeTable dh.TreeTable} of this table. Multiple calls to this
     * method will each produce a new {@link JsTreeTable dh.TreeTable} which must have {@link JsTreeTable#close()
     * close()} called on it when not in use.
     *
     * @param configObject specifies which column to use as the row ID, and which for parent ID, and how to handle rows
     *        with no parent
     * @return Promise of {@link JsTreeTable dh.TreeTable}
     */
    @JsMethod
    default Promise<JsTreeTable> createTreeTable(@TsTypeRef(JsTreeTableConfig.class) Object configObject) {
        JsTable t = createMutableTable();
        try {
            return t.treeTable(configObject).finally_(t::close);
        } catch (RuntimeException e) {
            t.close();
            throw e;
        }
    }

    /**
     * A promise that will resolve to a new roll-up {@link JsTreeTable dh.TreeTable} of this table. Multiple calls to
     * this method will each produce a new {@link JsTreeTable dh.TreeTable} which must have {@link JsTreeTable#close()
     * close()} called on it when not in use.
     *
     * @param configObject specifies which columns to aggregate and which to group by, and other rollup behavior
     * @return Promise of {@link JsTreeTable dh.TreeTable}
     */
    @JsMethod
    default Promise<JsTreeTable> createRollup(@TsTypeRef(JsRollupConfig.class) Object configObject) {
        JsTable t = createMutableTable();
        try {
            return t.rollup(configObject).finally_(t::close);
        } catch (RuntimeException e) {
            t.close();
            throw e;
        }
    }
}

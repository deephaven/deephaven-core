//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * The decoded values of some of a page's rows, cached in place of the whole page. Immutable.
 */
final class SparsePage<ATTR extends Any> extends PageCache.IntrusivePage<ATTR> {

    /** Page-relative rows. */
    final RowSet rows;
    /** An array of the values of {@link #rows}, in order. */
    final Object values;

    SparsePage(@NotNull final RowSet rows, @NotNull final Object values) {
        this.rows = rows;
        this.values = values;
    }
}

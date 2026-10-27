//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import java.util.Map;

/**
 * This interface marks WhereFilters that can produce a copy that reads the filter's columns under other names. A
 * {@link io.deephaven.engine.table.impl.DeferredViewTable DeferredViewTable} uses the copy to move a filter written
 * after a view to before the view, so that the view's source table can apply the filter before coalescing.
 * <p>
 * Only single filters implement this interface. Filters that wrap or combine other filters (NOT, serial, barriers, AND,
 * OR) do not: the {@code DeferredViewTable} visitor that executes filters before a view renames the single filters
 * inside them and decides for each wrapper whether the result may be executed before the view. An AND or OR may be
 * executed before the view only if every sub-filter can be renamed, and a serial filter never is, since a serial filter
 * must stay in order with the other filters. Both are decisions about where a filter is executed; a renamed copy is
 * used only when the whole filter can be executed before the view.
 */
public interface RenameableFilter extends WhereFilter {

    /**
     * Produce a copy of this filter that reads the filter's columns through {@code renames}.
     * <p>
     * The copy must resolve every name the way this filter resolved the name when initialized against the table the
     * filter was written for. A name that this filter read as a column reads the renamed column. A name that this
     * filter did not read as a column, such as a query-scope variable, keeps that meaning in the copy, even where the
     * table being filtered has a column of the same name: a view may hide a column of the view's source table, and a
     * renamed filter must not read a hidden column in place of a variable.
     * <p>
     * This filter must already be initialized.
     *
     * @param renames map from the column names this filter reads, as reported by {@link #getColumns()} and
     *        {@link #getColumnArrays()}, to the names in the table being filtered. The map need not be total: a column
     *        absent from the map keeps the same name. A disjunction or conjunction passes one map to every sub-filter,
     *        so a sub-filter may receive a map that names only the columns of the sub-filter's siblings.
     * @return the renamed filter
     */
    WhereFilter renameFilter(Map<String, String> renames);
}

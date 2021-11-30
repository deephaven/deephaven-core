/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import org.jetbrains.annotations.Nullable;

/**
 * This interface marks WhereFilters that return a result set that should be the full set for subsequent filters. Said
 * another way, subsequent filters need to see the RowSet selected by this filter for purposes of determining i values.
 * ReindexingFilters may also optionally specify a re-sorting of the table to be input.
 */
public interface ReindexingFilter extends WhereFilter {

    /**
     * @return True iff getSortColumns will return a non-null, non-empty array of column names to sort on.
     */
    boolean requiresSorting();

    /**
     * Get the columns on which the input table should be sorted before filtering.
     *
     * @return Columns to sort on, or null if there are no such columns
     */
    @Nullable
    String[] getSortColumns();

    /**
     * Advise this filter that sorting has been performed. requiresSorting must return false hereafter.
     */
    void sortingDone();
}

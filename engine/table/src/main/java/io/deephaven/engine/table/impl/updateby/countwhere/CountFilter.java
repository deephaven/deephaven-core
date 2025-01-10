//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.countwhere;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.ExposesChunkFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.SafeCloseable;

import java.util.*;

public class CountFilter {
    final ChunkFilter chunkFilter;
    final AbstractConditionFilter.Filter conditionFilter;
    final WhereFilter whereFilter;
    final int[] inputColumnIndices;

    public CountFilter(ChunkFilter chunkFilter, int[] inputColumnIndices) {
        this.chunkFilter = chunkFilter;
        this.conditionFilter = null;
        this.whereFilter = null;
        this.inputColumnIndices = inputColumnIndices;
    }

    public CountFilter(AbstractConditionFilter.Filter conditionFilter, int[] inputColumnIndices) {
        this.chunkFilter = null;
        this.conditionFilter = conditionFilter;
        this.whereFilter = null;
        this.inputColumnIndices = inputColumnIndices;
    }

    public CountFilter(WhereFilter whereFilter, int[] inputColumnIndices) {
        this.chunkFilter = null;
        this.conditionFilter = null;
        this.whereFilter = whereFilter;
        this.inputColumnIndices = inputColumnIndices;
    }

    /**
     * Create CountFilters from WhereFilter array, initializing the filters against the provided table.
     */
    public static CountFilter[] createCountFilters(
            final WhereFilter[] filters,
            final Table inputTable,
            final List<int[]> filterInputIndices) {

        // Create the internal filters
        final List<CountFilter> filterList = new ArrayList<>();
        boolean forcedWhereFilter = false;
        for (int fi = 0; fi < filters.length; fi++) {
            final WhereFilter filter = filters[fi];
            final CountFilter countFilter;
            if (!forcedWhereFilter && filter instanceof ConditionFilter) {
                final ConditionFilter conditionFilter = (ConditionFilter) filter;
                if (conditionFilter.hasVirtualRowVariables()) {
                    throw new UnsupportedOperationException(
                            "UpdateBy CountWhere operator does not support refreshing filters");
                }
                try {
                    countFilter = new CountFilter(conditionFilter.getFilter(inputTable, RowSetFactory.empty()),
                            filterInputIndices.get(fi));
                } catch (final Exception e) {
                    throw new IllegalArgumentException(
                            "Error creating condition filter in UpdateBy CountWhere Operator", e);
                }
            } else if (!forcedWhereFilter && filter instanceof ExposesChunkFilter
                    && ((ExposesChunkFilter) filter).chunkFilter().isPresent()) {
                final Optional<ChunkFilter> chunkFilter = ((ExposesChunkFilter) filter).chunkFilter();
                countFilter = new CountFilter(chunkFilter.get(), filterInputIndices.get(fi));
            } else {
                try (final SafeCloseable ignored = filter.beginOperation(inputTable)) {
                    countFilter = new CountFilter(filter, filterInputIndices.get(fi));
                }
                forcedWhereFilter = true;
            }
            filterList.add(countFilter);
        }

        return filterList.toArray(CountFilter[]::new);
    }

    public ChunkFilter chunkFilter() {
        return chunkFilter;
    }

    public AbstractConditionFilter.Filter conditionFilter() {
        return conditionFilter;
    }

    public WhereFilter whereFilter() {
        return whereFilter;
    }
}

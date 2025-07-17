//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

/**
 * A WhereFilter that always returns all rows in the selection.
 * <p>
 * This filter is used when no filtering is required, effectively passing through the entire selection. Using it allows
 * us to validate respectsBarriers even when a barrier was systematically moved to an operation earlier in the query
 * pipeline.
 */
public class WhereAllFilter extends WhereFilterImpl {
    public static final WhereAllFilter INSTANCE = new WhereAllFilter();

    private WhereAllFilter() {}

    @Override
    public List<String> getColumns() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {}

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return selection.copy();
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return RowSetFactory.empty();
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(WhereFilter.RecomputeListener result) {}

    @Override
    public WhereFilter copy() {
        return INSTANCE;
    }

    @Override
    public boolean canMemoize() {
        return true;
    }

    @Override
    public String toString() {
        return "WhereAllFilter";
    }
}

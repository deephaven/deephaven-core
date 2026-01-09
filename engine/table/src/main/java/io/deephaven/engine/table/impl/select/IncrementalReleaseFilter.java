//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import org.jetbrains.annotations.NotNull;

/**
 * Filters a table by permitting an initial number of rows, then adding a fixed number of rows on each update graph
 * cycle.
 *
 * <p>
 * The source table must be {@link Table#ADD_ONLY_TABLE_ATTRIBUTE add only}. If the source table is static or
 * {@link Table#APPEND_ONLY_TABLE_ATTRIBUTE append-only}, then the result table starts with the first
 * {@code initialSize} rows of the table. On the next cycle, it contains the first {@code initialSize + sizeIncrement}
 * rows. On the n-th cycle, it contains the first {@code initialSize + (n * sizeIncrement)} rows until the entire table
 * has been released. Once the entire table has been released, an append-only table permits no more than
 * {@code sizeIncrement} rows to be added each cycle. If more than {@code sizeIncrement} rows are added to the source,
 * then those rows are split across subsequent cycles until the full table is released. The released rows are always a
 * prefix of the source table.
 * </p>
 *
 * <p>
 * If the source table is add-only, the initial result contains the first {@code initialSize} rows of the table. On each
 * subsequent cycle, the result contains all rows that were in the result on the prior cycle, plus up to
 * {@code sizeIncrement} more rows (selected from the beginning of the table). The released rows may not be a prefix of
 * the source table. If more than {@code sizeIncrement} rows are added to the source, then those rows are split across
 * subsequent cycles until the full table is released.
 * </p>
 */
public class IncrementalReleaseFilter extends BaseIncrementalReleaseFilter {
    private final long sizeIncrement;
    private final WritableRowSet previouslyReleased = RowSetFactory.empty();

    /**
     * Create an incremental release filter with an initial size that will release sizeIncrement rows each cycle.
     *
     * @param initialSize how many rows should be in the table initially
     * @param sizeIncrement how many rows to release at the beginning of each UGP cycle.
     */
    public IncrementalReleaseFilter(long initialSize, long sizeIncrement) {
        super(initialSize, true);
        this.sizeIncrement = sizeIncrement;
    }

    @Override
    long getSizeIncrement() {
        return sizeIncrement;
    }

    @Override
    public @NotNull WritableRowSet filter(@NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table,
            boolean usePrev) {
        if (!(table instanceof BaseTable)) {
            throw new IllegalStateException("Table must be an instance of BaseTable!");
        }
        if (((BaseTable) table).isAppendOnly()) {
            return super.filter(selection, fullSet, table, usePrev);
        }

        if (usePrev) {
            Assert.eqZero(previouslyReleased.size(), "previouslyReleased.size()");
            try (final WritableRowSet releasedOnFirstStep = selection.subSetByPositionRange(0, getReleasedSize())) {
                previouslyReleased.insert(releasedOnFirstStep);
            }
            return previouslyReleased;
        }

        setExpectedSize(fullSet.size());

        if (getReleaseMoreRows()) {
            incrementReleasedSize(getSizeIncrement());
        }

        if (fullSet.size() <= getReleasedSize()) {
            setReleasedSize(fullSet.size());
            onReleaseAll();
            removeFromUpdateGraph(table);
        } else {
            // add it back for a refreshing table that has not been completely released
            addToUpdateGraph();
        }

        final long previousSize = previouslyReleased.size();
        final long releasedSize = getReleasedSize();
        if (previousSize < releasedSize) {
            final long newlyReleasedRows = releasedSize - previousSize;
            try (final WritableRowSet relevantRows = fullSet.minus(previouslyReleased);
                    final WritableRowSet toRelease = relevantRows.subSetByPositionRange(0, newlyReleasedRows)) {
                previouslyReleased.insert(toRelease);
            }
        }

        return previouslyReleased.intersect(selection);
    }
}

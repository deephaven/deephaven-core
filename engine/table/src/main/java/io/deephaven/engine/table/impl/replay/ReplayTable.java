/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.replay;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class ReplayTable extends QueryTable implements Runnable {
    /**
     * Creates a new ReplayTable based on a row set, set of column sources, time column, and a replayer
     */
    private final Replayer replayer;
    private final ColumnSource<Long> nanoTimeSource;
    private final RowSet.Iterator rowSetIterator;

    private long nextRowKey = RowSequence.NULL_ROW_KEY;
    private long currentTimeNanos = QueryConstants.NULL_LONG;
    private long nextTimeNanos = QueryConstants.NULL_LONG;
    private boolean done;

    public ReplayTable(
            @NotNull final RowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columns,
            @NotNull final String timeColumn,
            @NotNull final Replayer replayer) {
        super(RowSetFactory.empty().toTracking(), columns);
        this.replayer = Require.neqNull(replayer, "replayer");
        // NB: This will behave incorrectly if our row set or any data in columns can change. Our source table *must*
        // be static. We also seem to be assuming that timeSource has no null values in rowSet. It would be nice to use
        // a column iterator for this, but that would upset unit tests by keeping pooled chunks across cycles.
        final ColumnSource<DateTime> dateTimeSource = getColumnSource(timeColumn, DateTime.class);
        replayer.registerTimeSource(rowSet, dateTimeSource);
        nanoTimeSource = ReinterpretUtils.dateTimeToLongSource(dateTimeSource);
        rowSetIterator = rowSet.iterator();

        setRefreshing(true);

        advanceIterators();
        if (!done) {
            try (final RowSet initial = advanceToCurrentTime()) {
                getRowSet().writableCast().insert(initial);
            }
        }
    }

    /**
     * Advance the row key and time iterators if there are any left in the table.
     * 
     * @throws RuntimeException if time is null, or if the next time is before the current time.
     */
    private void advanceIterators() {
        if (rowSetIterator.hasNext()) {
            nextRowKey = rowSetIterator.nextLong();
            currentTimeNanos = nextTimeNanos;
            nextTimeNanos = nanoTimeSource.getLong(nextRowKey);
            if (nextTimeNanos == QueryConstants.NULL_LONG || nextTimeNanos < currentTimeNanos) {
                throw new RuntimeException(
                        "The historical table contains a null or decreasing time that cannot be replayed.");
            }
        } else {
            // NB: It would be best to ensure that if this is hit during construction, we're never added to the UGP.
            // If this is hit during update processing, it would be great to remove ourselves from the UGP right away.
            done = true;
        }
    }

    /**
     * Advance iterators to the current time.
     */
    private RowSet advanceToCurrentTime() {
        final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();
        final long currentReplayTimeNanos = replayer.clock().currentTimeNanos();
        while (!done && nextTimeNanos <= currentReplayTimeNanos) {
            addedBuilder.appendKey(nextRowKey);
            advanceIterators();
        }
        return addedBuilder.build();
    }

    @Override
    public void run() {
        if (done) {
            return;
        }
        final RowSet added = advanceToCurrentTime();
        if (added.isNonempty()) {
            getRowSet().writableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        } else {
            added.close();
        }
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.util.*;

import java.util.Map;

public class ReplayGroupedFullTable extends QueryReplayGroupedTable {
    private int redirIndexSize;

    public ReplayGroupedFullTable(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> input,
            String timeColumn,
            Replayer replayer, String groupingColumn) {
        super(rowSet, input, timeColumn, replayer,
                WritableRowRedirection.FACTORY.createRowRedirection((int) rowSet.size()),
                new String[] {groupingColumn});
        redirIndexSize = 0;
        // We do not modify existing entries in the WritableRowRedirection (we only add at the end), so there's no need
        // to
        // ask the WritableRowRedirection to track previous values.
    }

    @Override
    public void run() {
        if (allIterators.isEmpty()) {
            return;
        }
        RowSetBuilderRandom rowSetBuilder = RowSetFactory.builderRandom();
        while (!allIterators.isEmpty() && allIterators.peek().lastTime.getNanos() < replayer.currentTimeNanos()) {
            IteratorsAndNextTime currentIt = allIterators.poll();
            final long key = redirIndexSize++;
            rowRedirection.put(key, currentIt.lastIndex);
            rowSetBuilder.addKey(key);
            currentIt = currentIt.next();
            if (currentIt != null) {
                allIterators.add(currentIt);
            }
        }
        final RowSet added = rowSetBuilder.build();
        if (added.size() > 0) {
            getRowSet().writableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }
    }

}

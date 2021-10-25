/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.replay;

import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.RedirectionIndex;

import java.util.Map;

public class ReplayGroupedFullTable extends QueryReplayGroupedTable {
    private int redirIndexSize;

    public ReplayGroupedFullTable(TrackingMutableRowSet rowSet, Map<String, ? extends ColumnSource<?>> input, String timeColumn,
                                  Replayer replayer, String groupingColumn) {
        super(rowSet, input, timeColumn, replayer, RedirectionIndex.FACTORY.createRedirectionIndex((int) rowSet.size()),
                new String[] {groupingColumn});
        redirIndexSize = 0;
        // We do not modify existing entries in the RedirectionIndex (we only add at the end), so there's no need to
        // ask the RedirectionIndex to track previous values.
    }

    @Override
    public void refresh() {
        if (allIterators.isEmpty()) {
            return;
        }
        RowSetBuilderRandom rowSetBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        while (!allIterators.isEmpty() && allIterators.peek().lastTime.getNanos() < replayer.currentTimeNanos()) {
            IteratorsAndNextTime currentIt = allIterators.poll();
            final long key = redirIndexSize++;
            redirectionIndex.put(key, currentIt.lastIndex);
            rowSetBuilder.addKey(key);
            currentIt = currentIt.next();
            if (currentIt != null) {
                allIterators.add(currentIt);
            }
        }
        final TrackingMutableRowSet added = rowSetBuilder.build();
        if (added.size() > 0) {
            getIndex().insert(added);
            notifyListeners(added, RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), RowSetFactoryImpl.INSTANCE.getEmptyRowSet());
        }
    }

}

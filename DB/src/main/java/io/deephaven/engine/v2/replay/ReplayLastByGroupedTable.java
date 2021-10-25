/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.replay;

import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.RedirectionIndex;

import java.util.Map;

public class ReplayLastByGroupedTable extends QueryReplayGroupedTable {

    public ReplayLastByGroupedTable(TrackingMutableRowSet rowSet, Map<String, ? extends ColumnSource<?>> input, String timeColumn,
                                    Replayer replayer, String[] groupingColumns) {
        super(rowSet, input, timeColumn, replayer, RedirectionIndex.FACTORY.createRedirectionIndex(100),
                groupingColumns);
        // noinspection unchecked
        replayer.registerTimeSource(rowSet, (ColumnSource<DBDateTime>) input.get(timeColumn));
    }

    @Override
    public void refresh() {
        if (allIterators.isEmpty()) {
            return;
        }
        RowSetBuilderRandom addedBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        RowSetBuilderRandom modifiedBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        // List<IteratorsAndNextTime> iteratorsToAddBack = new ArrayList<>(allIterators.size());
        while (!allIterators.isEmpty() && allIterators.peek().lastTime.getNanos() < replayer.currentTimeNanos()) {
            IteratorsAndNextTime currentIt = allIterators.poll();
            redirectionIndex.put(currentIt.pos, currentIt.lastIndex);
            if (getRowSet().find(currentIt.pos) >= 0) {
                modifiedBuilder.addKey(currentIt.pos);
            } else {
                addedBuilder.addKey(currentIt.pos);
            }
            do {
                currentIt = currentIt.next();
            } while (currentIt != null && currentIt.lastTime.getNanos() < replayer.currentTimeNanos());
            if (currentIt != null) {
                allIterators.add(currentIt);
            }
        }
        final TrackingMutableRowSet added = addedBuilder.build();
        final TrackingMutableRowSet modified = modifiedBuilder.build();
        if (added.size() > 0 || modified.size() > 0) {
            getRowSet().insert(added);
            notifyListeners(added, RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), modified);
        }
    }
}

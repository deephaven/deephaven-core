//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.replay;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;

public class ReplayLastByGroupedTable extends QueryReplayGroupedTable {

    public ReplayLastByGroupedTable(Table source, String timeColumn, Replayer replayer, String[] groupingColumns) {
        super("ReplayLastByGroupedTable", source, timeColumn, replayer,
                WritableRowRedirection.FACTORY.createRowRedirection(100), groupingColumns);
        replayer.registerTimeSource(source.getRowSet(), source.getColumnSource(timeColumn, Instant.class));
    }

    @Override
    public void run() {
        if (allIterators.isEmpty()) {
            return;
        }
        RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
        RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();
        // List<IteratorsAndNextTime> iteratorsToAddBack = new ArrayList<>(allIterators.size());
        while (!allIterators.isEmpty()
                && DateTimeUtils.epochNanos(allIterators.peek().lastTime) < replayer.clock().currentTimeNanos()) {
            IteratorsAndNextTime currentIt = allIterators.poll();
            rowRedirection.put(currentIt.pos, currentIt.lastIndex);
            if (getRowSet().find(currentIt.pos) >= 0) {
                modifiedBuilder.addKey(currentIt.pos);
            } else {
                addedBuilder.addKey(currentIt.pos);
            }
            do {
                currentIt = currentIt.next();
            } while (currentIt != null
                    && DateTimeUtils.epochNanos(currentIt.lastTime) < replayer.clock().currentTimeNanos());
            if (currentIt != null) {
                allIterators.add(currentIt);
            }
        }
        final RowSet added = addedBuilder.build();
        final RowSet modified = modifiedBuilder.build();
        if (!added.isEmpty() || !modified.isEmpty()) {
            getRowSet().writableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), modified);
        }
    }
}

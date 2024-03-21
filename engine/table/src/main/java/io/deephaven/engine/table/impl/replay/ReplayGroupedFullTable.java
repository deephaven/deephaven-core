//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.replay;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.time.DateTimeUtils;

public class ReplayGroupedFullTable extends QueryReplayGroupedTable {

    private int redirIndexSize;

    public ReplayGroupedFullTable(Table source, String timeColumn, Replayer replayer, String groupingColumn) {
        super("ReplayGroupedFullTable", source, timeColumn, replayer,
                WritableRowRedirection.FACTORY.createRowRedirection(source.intSize()), new String[] {groupingColumn});
        redirIndexSize = 0;
        // We do not modify existing entries in the WritableRowRedirection (we only add at the end), so there's no need
        // to ask the WritableRowRedirection to track previous values.
    }

    @Override
    public void run() {
        if (allIterators.isEmpty()) {
            return;
        }
        RowSetBuilderRandom rowSetBuilder = RowSetFactory.builderRandom();
        while (!allIterators.isEmpty()
                && DateTimeUtils.epochNanos(allIterators.peek().lastTime) < replayer.clock().currentTimeNanos()) {
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
        if (!added.isEmpty()) {
            getRowSet().writableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }
    }

}

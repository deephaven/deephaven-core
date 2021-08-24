/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.replay;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexBuilder;
import io.deephaven.db.v2.utils.RedirectionIndex;

import java.util.Map;

public class ReplayLastByGroupedTable extends QueryReplayGroupedTable {

    public ReplayLastByGroupedTable(Index index, Map<String, ? extends ColumnSource> input,
        String timeColumn, Replayer replayer, String[] groupingColumns) {
        super(index, input, timeColumn, replayer,
            RedirectionIndex.FACTORY.createRedirectionIndex(100), groupingColumns);
        replayer.registerTimeSource(index, input.get(timeColumn));
    }

    @Override
    public void refresh() {
        if (allIterators.isEmpty()) {
            return;
        }
        IndexBuilder addedBuilder = Index.FACTORY.getBuilder();
        IndexBuilder modifiedBuilder = Index.FACTORY.getBuilder();
        // List<IteratorsAndNextTime> iteratorsToAddBack = new ArrayList<>(allIterators.size());
        while (!allIterators.isEmpty()
            && allIterators.peek().lastTime.getNanos() < replayer.currentTimeNanos()) {
            IteratorsAndNextTime currentIt = allIterators.poll();
            redirectionIndex.put(currentIt.pos, currentIt.lastIndex);
            if (getIndex().find(currentIt.pos) >= 0) {
                modifiedBuilder.addKey(currentIt.pos);
            } else {
                addedBuilder.addKey(currentIt.pos);
            }
            do {
                currentIt = currentIt.next();
            } while (currentIt != null
                && currentIt.lastTime.getNanos() < replayer.currentTimeNanos());
            if (currentIt != null) {
                allIterators.add(currentIt);
            }
        }
        final Index added = addedBuilder.getIndex();
        final Index modified = modifiedBuilder.getIndex();
        if (added.size() > 0 || modified.size() > 0) {
            getIndex().insert(added);
            notifyListeners(added, Index.FACTORY.getEmptyIndex(), modified);
        }
    }
}

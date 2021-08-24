/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.replay;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexBuilder;
import io.deephaven.db.v2.utils.RedirectionIndex;

import java.util.Map;

public class ReplayGroupedFullTable extends QueryReplayGroupedTable {
    private int redirIndexSize;

    public ReplayGroupedFullTable(Index index, Map<String, ? extends ColumnSource> input,
        String timeColumn, Replayer replayer, String groupingColumn) {
        super(index, input, timeColumn, replayer,
            RedirectionIndex.FACTORY.createRedirectionIndex((int) index.size()),
            new String[] {groupingColumn});
        redirIndexSize = 0;
        // We do not modify existing entries in the RedirectionIndex (we only add at the end), so
        // there's no need to
        // ask the RedirectionIndex to track previous values.
    }

    @Override
    public void refresh() {
        if (allIterators.isEmpty()) {
            return;
        }
        IndexBuilder indexBuilder = Index.FACTORY.getBuilder();
        while (!allIterators.isEmpty()
            && allIterators.peek().lastTime.getNanos() < replayer.currentTimeNanos()) {
            IteratorsAndNextTime currentIt = allIterators.poll();
            final long key = redirIndexSize++;
            redirectionIndex.put(key, currentIt.lastIndex);
            indexBuilder.addKey(key);
            currentIt = currentIt.next();
            if (currentIt != null) {
                allIterators.add(currentIt);
            }
        }
        final Index added = indexBuilder.getIndex();
        if (added.size() > 0) {
            getIndex().insert(added);
            notifyListeners(added, Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex());
        }
    }

}

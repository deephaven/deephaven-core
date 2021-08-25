/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.replay;


import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.RedirectionIndex;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;

public abstract class QueryReplayGroupedTable extends QueryTable implements LiveTable {


    protected final RedirectionIndex redirectionIndex;
    final Replayer replayer;
    protected PriorityQueue<IteratorsAndNextTime> allIterators = new PriorityQueue<>();

    private static Map<String, ColumnSource> getResultSources(
        Map<String, ? extends ColumnSource> input, RedirectionIndex redirectionIndex) {
        Map<String, ColumnSource> result = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends ColumnSource> stringEntry : input.entrySet()) {
            ColumnSource value = stringEntry.getValue();
            result.put(stringEntry.getKey(),
                new ReadOnlyRedirectedColumnSource<>(redirectionIndex, value));
        }
        return result;
    }

    static class IteratorsAndNextTime implements Comparable {

        private final Index.Iterator iterator;
        private final ColumnSource<DBDateTime> columnSource;
        DBDateTime lastTime;
        long lastIndex;
        public final long pos;

        private IteratorsAndNextTime(Index.Iterator iterator, ColumnSource<DBDateTime> columnSource,
            long pos) {
            this.iterator = iterator;
            this.columnSource = columnSource;
            this.pos = pos;
            lastIndex = iterator.nextLong();
            lastTime = columnSource.get(lastIndex);
        }

        IteratorsAndNextTime next() {
            if (iterator.hasNext()) {
                lastIndex = iterator.nextLong();
                lastTime = columnSource.get(lastIndex);
                return this;
            } else {
                return null;
            }
        }

        @Override
        public int compareTo(Object o) {
            if (lastTime == null) {
                return ((IteratorsAndNextTime) o).lastTime == null ? 0 : -1;
            }
            return lastTime.compareTo(((IteratorsAndNextTime) o).lastTime);
        }
    }

    protected QueryReplayGroupedTable(Index index, Map<String, ? extends ColumnSource> input,
        String timeColumn, Replayer replayer, RedirectionIndex redirectionIndex,
        String[] groupingColumns) {

        super(Index.FACTORY.getIndexByValues(), getResultSources(input, redirectionIndex));
        this.redirectionIndex = redirectionIndex;
        Map<Object, Index> grouping;

        final ColumnSource[] columnSources =
            Arrays.stream(groupingColumns).map(gc -> input.get(gc)).toArray(ColumnSource[]::new);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(columnSources);
        grouping = index.getGrouping(tupleSource);

        @SuppressWarnings("unchecked")
        ColumnSource<DBDateTime> timeSource = input.get(timeColumn);
        int pos = 0;
        for (Index groupIndex : grouping.values()) {
            Index.Iterator iterator = groupIndex.iterator();
            if (iterator.hasNext()) {
                allIterators.add(new IteratorsAndNextTime(iterator, timeSource, pos++));
            }
        }
        Require.requirement(replayer != null, "replayer != null");
        setRefreshing(true);
        this.replayer = replayer;
        refresh();
    }
}

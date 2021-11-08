/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.replay;


import io.deephaven.base.verify.Require;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.engine.v2.tuples.TupleSourceFactory;
import io.deephaven.engine.v2.utils.*;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;

public abstract class QueryReplayGroupedTable extends QueryTable implements Runnable {


    protected final RedirectionIndex redirectionIndex;
    final Replayer replayer;
    protected PriorityQueue<IteratorsAndNextTime> allIterators = new PriorityQueue<>();

    private static Map<String, ColumnSource<?>> getResultSources(Map<String, ? extends ColumnSource<?>> input,
            RedirectionIndex redirectionIndex) {
        Map<String, ColumnSource<?>> result = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends ColumnSource<?>> stringEntry : input.entrySet()) {
            ColumnSource<?> value = stringEntry.getValue();
            result.put(stringEntry.getKey(), new ReadOnlyRedirectedColumnSource<>(redirectionIndex, value));
        }
        return result;
    }

    static class IteratorsAndNextTime implements Comparable<IteratorsAndNextTime> {

        private final RowSet.Iterator iterator;
        private final ColumnSource<DBDateTime> columnSource;
        DBDateTime lastTime;
        long lastIndex;
        public final long pos;

        private IteratorsAndNextTime(RowSet.Iterator iterator, ColumnSource<DBDateTime> columnSource, long pos) {
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
        public int compareTo(IteratorsAndNextTime o) {
            if (lastTime == null) {
                return o.lastTime == null ? 0 : -1;
            }
            return lastTime.compareTo(o.lastTime);
        }
    }

    protected QueryReplayGroupedTable(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> input,
            String timeColumn, Replayer replayer, RedirectionIndex redirectionIndex, String[] groupingColumns) {

        super(RowSetFactory.empty().toTracking(), getResultSources(input, redirectionIndex));
        this.redirectionIndex = redirectionIndex;
        Map<Object, RowSet> grouping;

        final ColumnSource<?>[] columnSources =
                Arrays.stream(groupingColumns).map(input::get).toArray(ColumnSource[]::new);
        final TupleSource<?> tupleSource = TupleSourceFactory.makeTupleSource(columnSources);
        grouping = rowSet.getGrouping(tupleSource);

        // noinspection unchecked
        ColumnSource<DBDateTime> timeSource = (ColumnSource<DBDateTime>) input.get(timeColumn);
        int pos = 0;
        for (RowSet groupRowSet : grouping.values()) {
            RowSet.Iterator iterator = groupRowSet.iterator();
            if (iterator.hasNext()) {
                allIterators.add(new IteratorsAndNextTime(iterator, timeSource, pos++));
            }
        }
        Require.requirement(replayer != null, "replayer != null");
        setRefreshing(true);
        this.replayer = replayer;
        run();
    }
}

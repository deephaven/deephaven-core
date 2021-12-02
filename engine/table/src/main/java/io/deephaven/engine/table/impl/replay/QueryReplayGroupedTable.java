/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;


import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.table.impl.util.*;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;

public abstract class QueryReplayGroupedTable extends QueryTable implements Runnable {


    protected final WritableRowRedirection rowRedirection;
    final Replayer replayer;
    protected PriorityQueue<IteratorsAndNextTime> allIterators = new PriorityQueue<>();

    private static Map<String, ColumnSource<?>> getResultSources(Map<String, ? extends ColumnSource<?>> input,
            WritableRowRedirection rowRedirection) {
        Map<String, ColumnSource<?>> result = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends ColumnSource<?>> stringEntry : input.entrySet()) {
            ColumnSource<?> value = stringEntry.getValue();
            result.put(stringEntry.getKey(), new RedirectedColumnSource<>(rowRedirection, value));
        }
        return result;
    }

    static class IteratorsAndNextTime implements Comparable<IteratorsAndNextTime> {

        private final RowSet.Iterator iterator;
        private final ColumnSource<DateTime> columnSource;
        DateTime lastTime;
        long lastIndex;
        public final long pos;

        private IteratorsAndNextTime(RowSet.Iterator iterator, ColumnSource<DateTime> columnSource, long pos) {
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
            String timeColumn, Replayer replayer, WritableRowRedirection rowRedirection, String[] groupingColumns) {

        super(RowSetFactory.empty().toTracking(), getResultSources(input, rowRedirection));
        this.rowRedirection = rowRedirection;
        Map<Object, RowSet> grouping;

        final ColumnSource<?>[] columnSources =
                Arrays.stream(groupingColumns).map(input::get).toArray(ColumnSource[]::new);
        final TupleSource<?> tupleSource = TupleSourceFactory.makeTupleSource(columnSources);
        grouping = RowSetIndexer.of(rowSet).getGrouping(tupleSource);

        // noinspection unchecked
        ColumnSource<DateTime> timeSource = (ColumnSource<DateTime>) input.get(timeColumn);
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

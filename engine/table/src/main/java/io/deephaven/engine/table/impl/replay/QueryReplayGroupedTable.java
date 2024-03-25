//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.replay;


import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

public abstract class QueryReplayGroupedTable extends ReplayTableBase implements Runnable {


    protected final WritableRowRedirection rowRedirection;
    final Replayer replayer;
    protected PriorityQueue<IteratorsAndNextTime> allIterators = new PriorityQueue<>();

    private static Map<String, ColumnSource<?>> getResultSources(Map<String, ? extends ColumnSource<?>> input,
            WritableRowRedirection rowRedirection) {
        Map<String, ColumnSource<?>> result = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends ColumnSource<?>> stringEntry : input.entrySet()) {
            ColumnSource<?> value = stringEntry.getValue();
            result.put(stringEntry.getKey(), RedirectedColumnSource.maybeRedirect(rowRedirection, value));
        }
        return result;
    }

    protected static class IteratorsAndNextTime implements Comparable<IteratorsAndNextTime> {

        private final RowSet.Iterator iterator;
        private final ColumnSource<Instant> columnSource;
        protected Instant lastTime;
        protected long lastIndex;
        public final long pos;

        private IteratorsAndNextTime(RowSet.Iterator iterator, ColumnSource<Instant> columnSource, long pos) {
            this.iterator = iterator;
            this.columnSource = columnSource;
            this.pos = pos;
            lastIndex = iterator.nextLong();
            lastTime = columnSource.get(lastIndex);
        }

        protected IteratorsAndNextTime next() {
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

    protected QueryReplayGroupedTable(
            @NotNull final String description,
            @NotNull final Table source,
            @NotNull final String timeColumn,
            @NotNull final Replayer replayer,
            @NotNull final WritableRowRedirection rowRedirection,
            @NotNull final String[] groupingColumns) {

        super(description, RowSetFactory.empty().toTracking(),
                getResultSources(source.getColumnSourceMap(), rowRedirection));
        this.rowRedirection = rowRedirection;
        this.replayer = Objects.requireNonNull(replayer, "replayer");


        final BasicDataIndex dataIndex = DataIndexer.getOrCreateDataIndex(source, groupingColumns);
        final Table indexTable = dataIndex.table();

        ColumnSource<Instant> timeSource = source.getColumnSource(timeColumn, Instant.class);
        int pos = 0;
        try (final CloseableIterator<RowSet> it = indexTable.columnIterator(dataIndex.rowSetColumnName())) {
            while (it.hasNext()) {
                RowSet.Iterator iterator = it.next().iterator();
                if (iterator.hasNext()) {
                    allIterators.add(new IteratorsAndNextTime(iterator, timeSource, pos++));
                }
            }
        }

        run();
    }
}

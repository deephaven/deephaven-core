package io.deephaven.server.table.stats;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import org.apache.commons.text.StringEscapeUtils;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Map;

public interface ChunkedComparableStatsKernel<T> {
    int CHUNK_SIZE = ChunkedNumericalStatsKernel.CHUNK_SIZE;

    static ChunkedComparableStatsKernel<?> makeChunkedComparableStatsFactory(final Class<?> type) {
        if (type == Character.class || type == char.class) {
            return new CharacterChunkedComparableStats();
        } else if (Comparable.class.isAssignableFrom(type)) {
            return new ObjectChunkedComparableStats(true);
        } else {
            return new ObjectChunkedComparableStats(false);
        }
    }

    static Result getChunkedComparableStats(final int maxUnique, final Table table, final String columnName, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        final RowSet index = usePrev ? table.getRowSet().prev() : table.getRowSet();

        final long startTime = System.currentTimeMillis();
        return makeChunkedComparableStatsFactory(columnSource.getType())
                .processChunks(index, columnSource, usePrev, maxUnique)
                .setRunTime(System.currentTimeMillis()-startTime);
    }

    Result processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev, int maxUnique);


    class Result implements Serializable {
        private final long size;
        private final long count;
        private final boolean isComparable;
        private final int numUnique;
        private final Map<String, Long> uniqueValues;

        private long runTime;

        Result(long size, long count, int numUnique, Map<String, Long> uniqueValues) {
            this.size = size;
            this.count = count;
            this.isComparable = true;
            this.numUnique = numUnique;
            this.uniqueValues = uniqueValues;
        }

        Result(long size, long count) {
            this.size = size;
            this.count = count;
            this.isComparable = false;
            this.numUnique = 0;
            this.uniqueValues = Collections.emptyMap();
        }

        private Result setRunTime(final long runTime) {
            this.runTime = runTime;
            return this;
        }

        public long getSize() {
            return size;
        }

        public long getCount() {
            return count;
        }

        public boolean isComparable() {
            return isComparable;
        }

        public int getNumUnique() {
            return numUnique;
        }

        public Map<String, Long> getUniqueValues() {
            return Collections.unmodifiableMap(uniqueValues);
        }
    }
}

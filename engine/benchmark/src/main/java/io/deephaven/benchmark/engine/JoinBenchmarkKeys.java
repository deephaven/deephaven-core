//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;

import java.util.Map;
import java.util.SplittableRandom;

/**
 * The key columns of the join benchmarks, built so that a fixed percentage of each side's rows find their key on the
 * other side, at every key count.
 * <p>
 * Each side holds {@code keyCount} distinct keys, numbered from zero. Key number {@code n} is shared with the other
 * side when {@code n % 100 < hitPercent}, and then has the value {@code n} on both sides; otherwise its value is
 * private to its side. A side's rows cover each of its keys equally often, in a shuffled order that differs between the
 * sides. So exactly {@code hitPercent} percent of either side's rows find their key on the other side, and the order in
 * which one side's keys first appear is unrelated to the order in which the other side looks them up.
 * <p>
 * The keys are generated once, before the benchmark, so that reading them costs only what reading any in-memory column
 * costs.
 */
final class JoinBenchmarkKeys {

    /** The fewest rows a side has; a side with more keys than this has one row per key. */
    static final int MIN_ROWS = 1_000_000;

    /** The side whose private keys follow the shared ones, and whose shuffle is seeded with 1. */
    static final int LEFT = 1;
    /** The side whose private keys follow the left's private keys, and whose shuffle is seeded with 2. */
    static final int RIGHT = 2;

    private JoinBenchmarkKeys() {}

    /**
     * @param keyCount the number of distinct keys on the side
     * @return the number of rows of a side with {@code keyCount} keys: at least {@link #MIN_ROWS}, and at least one row
     *         per key
     */
    static int sideRows(final int keyCount) {
        return Math.max(MIN_ROWS, keyCount);
    }

    /**
     * @param rows the number of rows, a multiple of {@code keyCount}
     * @param keyCount the number of distinct keys on the side, a multiple of 100
     * @param hitPercent the percentage of the side's keys, and so of its rows, that the other side shares
     * @param side {@link #LEFT} or {@link #RIGHT}
     * @return the key of each row
     */
    static long[] keys(final int rows, final int keyCount, final int hitPercent, final int side) {
        Require.eqZero(rows % keyCount, "rows % keyCount");
        Require.eqZero(keyCount % 100, "keyCount % 100");
        Require.inRange(hitPercent, "hitPercent", 101, "101");
        final long[] keys = new long[rows];
        for (int ii = 0; ii < rows; ++ii) {
            final int keyNumber = ii % keyCount;
            keys[ii] = keyNumber % 100 < hitPercent ? keyNumber : (long) keyCount * side + keyNumber;
        }
        // a Fisher-Yates shuffle, seeded by the side so that the two sides are shuffled differently
        final SplittableRandom random = new SplittableRandom(side);
        for (int ii = rows - 1; ii > 0; --ii) {
            final int jj = random.nextInt(ii + 1);
            final long swap = keys[ii];
            keys[ii] = keys[jj];
            keys[jj] = swap;
        }
        return keys;
    }

    /**
     * @return an in-memory column of {@code keys}
     */
    static ColumnSource<Long> keySource(final long[] keys) {
        return ArrayBackedColumnSource.getMemoryColumnSource(keys);
    }

    /**
     * @return a flat table of {@code rows} rows with {@code columns}, which a refreshing table keeps flat
     */
    static QueryTable table(final int rows, final Map<String, ? extends ColumnSource<?>> columns,
            final boolean refreshing) {
        final QueryTable table = new QueryTable(RowSetFactory.flat(rows).toTracking(), columns);
        table.setRefreshing(refreshing);
        if (refreshing) {
            table.setFlat();
        }
        return table;
    }

    /**
     * A long column whose value at a row key is the row key, which serves as a stamp in row order.
     */
    static final class RowKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {

        RowKeySource() {
            super(long.class);
        }

        @Override
        public long getLong(final long rowKey) {
            return rowKey;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return rowKey;
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }
}

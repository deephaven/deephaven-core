//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper;

import java.lang.reflect.Field;

/**
 * Shared pieces of {@link AggregationBuildBenchmark} and {@link AggregationIncrementalBenchmark}.
 */
final class AggregationStateBenchSupport {
    private AggregationStateBenchSupport() {}

    /**
     * Select how refreshing aggregations reclaim the states of removed keys: {@code none} keeps every state,
     * {@code compact} shifts states into the positions of removed ones, and {@code blocks} releases whole blocks of
     * empty states in place.
     *
     * <p>
     * The {@code ChunkedOperatorAggregationHelper} flags are set reflectively so that these benchmarks also compile and
     * run against a build that predates them; such a build supports only {@code none}.
     * </p>
     *
     * @param reclaim the reclaim mode
     */
    static void setReclaimMode(final String reclaim) {
        switch (reclaim) {
            case "none":
                setFlag("RECLAIM_STATES", false);
                break;
            case "compact":
                setFlag("RECLAIM_STATES", true);
                setFlag("RELEASE_BLOCKS", false);
                break;
            case "blocks":
                setFlag("RECLAIM_STATES", true);
                setFlag("RELEASE_BLOCKS", true);
                break;
            default:
                throw new IllegalArgumentException("Unknown reclaim mode " + reclaim);
        }
    }

    /**
     * Set the fraction free at which blocks of output positions are collapsed; 1 or more disables collapsing, which is
     * all that a build without the setting supports.
     *
     * @param collapseFreeFraction the fraction free at which a block is collapsed
     */
    static void setCollapseFreeFraction(final double collapseFreeFraction) {
        try {
            ChunkedOperatorAggregationHelper.class.getField("COLLAPSE_FREE_FRACTION").setDouble(null,
                    collapseFreeFraction);
        } catch (NoSuchFieldException e) {
            if (collapseFreeFraction < 1) {
                throw new IllegalStateException("This build cannot collapse blocks", e);
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Set the fraction of the output positions that released blocks at the start must reach before every state is
     * shifted down to reuse them; negative, which is all that a build without the setting supports, never shifts.
     *
     * @param frontShiftFraction the fraction at which to shift, zero for any released block, negative for never
     */
    static void setFrontShiftFraction(final double frontShiftFraction) {
        try {
            ChunkedOperatorAggregationHelper.class.getField("FRONT_SHIFT_FRACTION").setDouble(null, frontShiftFraction);
        } catch (NoSuchFieldException e) {
            if (frontShiftFraction >= 0) {
                throw new IllegalStateException("This build cannot shift states down", e);
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final String TOMBSTONE_STATE_MANAGER =
            "io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones";

    /**
     * Select whether removals migrate entries during a rehash; a build without the setting supports only false.
     *
     * @param migrateOnTombstone whether to migrate a live entry for each tombstone created in the main table
     */
    static void setMigrateOnTombstone(final boolean migrateOnTombstone) {
        try {
            Class.forName(TOMBSTONE_STATE_MANAGER).getField("MIGRATE_ON_TOMBSTONE").setBoolean(null,
                    migrateOnTombstone);
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            if (migrateOnTombstone) {
                throw new IllegalStateException("This build cannot migrate on tombstones", e);
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @return the number of hash table rehashes begun so far, or -1 if the build does not count them
     */
    static long rehashCount() {
        try {
            final Object adder = Class.forName(TOMBSTONE_STATE_MANAGER).getField("REHASH_COUNT").get(null);
            return ((java.util.concurrent.atomic.LongAdder) adder).sum();
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            return -1;
        }
    }

    private static void setFlag(final String name, final boolean value) {
        final Field field;
        try {
            field = ChunkedOperatorAggregationHelper.class.getField(name);
        } catch (NoSuchFieldException e) {
            if (value) {
                throw new IllegalStateException("This build does not support " + name, e);
            }
            return;
        }
        try {
            field.setBoolean(null, value);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A long column whose value is a pure function of the row key, so that adding or removing rows costs nothing in the
     * column itself. The value at {@code rowKey} is {@code rowKey / rowsPerValue}, reduced modulo {@code valueSpace}
     * when {@code valueSpace} is positive.
     */
    static final class ComputedLongSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long rowsPerValue;
        private final long valueSpace;

        ComputedLongSource(final long rowsPerValue, final long valueSpace) {
            super(long.class);
            this.rowsPerValue = rowsPerValue;
            this.valueSpace = valueSpace;
        }

        @Override
        public long getLong(final long rowKey) {
            final long value = rowKey / rowsPerValue;
            return valueSpace > 0 ? value % valueSpace : value;
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
        }

        @Override
        public boolean isImmutable() {
            return false;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }
}

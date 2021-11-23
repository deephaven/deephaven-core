/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class IterativeIndexSpec extends ReaggregatableStatefactory {
    static final String ROW_REDIRECTION_PREFIX = "RowRedirection_";
    private final Map<String, ColumnSource> nameToDestColumns = new LinkedHashMap<>();
    final protected RowRedirection rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(8);
    private boolean firstTime;
    final boolean lowestRollup;
    final boolean secondRollup;
    final int rollupColumnIdentifier;

    // if we have multiple aggregations, we do not want them to have conflicting column names, so we use an identifier
    // to find them
    private final static AtomicInteger nextRollupColumnIdentifier = new AtomicInteger(1);

    IterativeIndexSpec(boolean lowestRollup, boolean secondRollup, int rollupColumnIdentifier) {
        firstTime = true;
        this.lowestRollup = lowestRollup;
        this.secondRollup = secondRollup;
        if (lowestRollup) {
            this.rollupColumnIdentifier = nextRollupColumnIdentifier.getAndIncrement();
        } else {
            this.rollupColumnIdentifier = rollupColumnIdentifier;
        }
    }

    @NotNull
    private String getRedirectionName() {
        return ROW_REDIRECTION_PREFIX + rollupColumnIdentifier + AggregationFactory.ROLLUP_COLUMN_SUFFIX;
    }

    @Override
    boolean supportsRollup() {
        return true;
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        throw new UnsupportedOperationException();
    }

    @Override
    ReaggregatableStatefactory rollupFactory() {
        throw new UnsupportedOperationException();
    }

    class RedirectionValueColumnSource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        RedirectionValueColumnSource() {
            super(Long.class);
        }

        @Override
        public void startTrackingPrevValues() {
            // Nothing to do.
        }

        @Override
        public long getLong(long index) {
            return rowRedirection.get(index);
        }

        @Override
        public long getPrevLong(long index) {
            return rowRedirection.getPrev(index);
        }

        @Override
        public boolean isImmutable() {
            return false;
        }
    }
}

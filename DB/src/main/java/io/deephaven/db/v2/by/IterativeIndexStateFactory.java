/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.utils.RedirectionIndex;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class IterativeIndexStateFactory extends ReaggregatableStatefactory {
    static final String REDIRECTION_INDEX_PREFIX = "RedirectionIndex_";
    private final Map<String, ColumnSource> nameToDestColumns = new LinkedHashMap<>();
    final protected RedirectionIndex redirectionIndex = RedirectionIndex.FACTORY.createRedirectionIndex(8);
    private boolean firstTime;
    final boolean lowestRollup;
    final boolean secondRollup;
    final int rollupColumnIdentifier;

    // if we have multiple aggregations, we do not want them to have conflicting column names, so we use an identifier
    // to find them
    private final static AtomicInteger nextRollupColumnIdentifier = new AtomicInteger(1);

    IterativeIndexStateFactory(boolean lowestRollup, boolean secondRollup, int rollupColumnIdentifier) {
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
        return REDIRECTION_INDEX_PREFIX + rollupColumnIdentifier + ComboAggregateFactory.ROLLUP_COLUMN_SUFFIX;
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
            return redirectionIndex.get(index);
        }

        @Override
        public long getPrevLong(long index) {
            return redirectionIndex.getPrev(index);
        }

        @Override
        public boolean isImmutable() {
            return false;
        }
    }
}

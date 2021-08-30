/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import java.util.Objects;

/**
 * Aggregation state factory for countBy() operation.
 */
public class CountByStateFactoryImpl extends ReaggregatableStatefactory {
    private final String countColumnName;

    public CountByStateFactoryImpl(String countColumnName) {
        this.countColumnName = countColumnName;
    }

    public String getCountName() {
        return countColumnName;
    }

    private static class MemoKey implements AggregationMemoKey {
        private final String countColumnName;

        private MemoKey(String countColumnName) {
            this.countColumnName = countColumnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final MemoKey memoKey = (MemoKey) o;
            return Objects.equals(countColumnName, memoKey.countColumnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(countColumnName);
        }
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        return new MemoKey(countColumnName);
    }

    @Override
    boolean supportsRollup() {
        return true;
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        return this;
    }

    @Override
    ReaggregatableStatefactory rollupFactory() {
        return new SumStateFactory();
    }
}

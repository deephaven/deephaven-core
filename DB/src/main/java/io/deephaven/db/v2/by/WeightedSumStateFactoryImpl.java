/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import java.util.Objects;

public class WeightedSumStateFactoryImpl extends IterativeOperatorStateFactory {
    private final String weightName;

    public WeightedSumStateFactoryImpl(String weightName) {
        super();
        this.weightName = weightName;
    }

    public String getWeightName() {
        return weightName;
    }

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return null;
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

    private static class MemoKey implements AggregationMemoKey {
        private final String weightColumn;

        private MemoKey(String weightColumn) {
            this.weightColumn = weightColumn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final MemoKey memoKey = (MemoKey) o;
            return Objects.equals(weightColumn, memoKey.weightColumn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(weightColumn);
        }
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        return new MemoKey(weightName);
    }
}

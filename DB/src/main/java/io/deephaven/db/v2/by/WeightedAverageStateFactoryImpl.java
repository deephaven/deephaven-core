/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import java.util.Objects;

public class WeightedAverageStateFactoryImpl implements AggregationStateFactory {
    private final String weightName;

    public WeightedAverageStateFactoryImpl(String weightName) {
        this.weightName = weightName;
    }

    public String getWeightName() {
        return weightName;
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

    @Override
    public String toString() {
        return "WeightedAverageStateFactoryImpl(" + weightName + ")";
    }
}

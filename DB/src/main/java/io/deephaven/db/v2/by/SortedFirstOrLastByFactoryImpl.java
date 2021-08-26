/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Require;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class SortedFirstOrLastByFactoryImpl extends IterativeIndexStateFactory
    implements Serializable {

    final private String[] sortColumnNames;
    final private boolean minimum;

    SortedFirstOrLastByFactoryImpl(boolean minimum, String... sortColumnNames) {
        this(minimum, false, false, 0, sortColumnNames);
    }

    SortedFirstOrLastByFactoryImpl(boolean minimum, boolean firstRollup, boolean secondRollup,
        int rollupIdentifier, String... sortColumnNames) {
        super(firstRollup, secondRollup, rollupIdentifier);
        Require.gtZero(sortColumnNames.length, "sortColumnNames.length");
        this.sortColumnNames = sortColumnNames;
        this.minimum = minimum;
    }

    public String[] getSortColumnNames() {
        return sortColumnNames;
    }

    private static final class MemoKey implements AggregationMemoKey {
        private final boolean minimum;
        private final String[] sortColumnNames;

        private MemoKey(boolean minimum, String[] sortColumnNames) {
            this.minimum = minimum;
            this.sortColumnNames = sortColumnNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final MemoKey memoKey = (MemoKey) o;
            return minimum == memoKey.minimum &&
                Arrays.equals(sortColumnNames, memoKey.sortColumnNames);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(minimum);
            result = 31 * result + Arrays.hashCode(sortColumnNames);
            return result;
        }
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        return new MemoKey(minimum, sortColumnNames);
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        return new SortedFirstOrLastByFactoryImpl(minimum, true, false, 0, sortColumnNames);
    }

    @Override
    ReaggregatableStatefactory rollupFactory() {
        return new SortedFirstOrLastByFactoryImpl(minimum, false, true, 0, sortColumnNames);
    }

    public boolean isSortedFirst() {
        return minimum;
    }
}

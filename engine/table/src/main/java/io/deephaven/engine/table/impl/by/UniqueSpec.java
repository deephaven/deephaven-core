package io.deephaven.engine.table.impl.by;

import java.util.Objects;

/**
 * An Iterative state factory that displays the singular unique value of the items within a particular state, or default
 * values if none are present, or the values are not unique.
 */
public class UniqueSpec extends IterativeOperatorSpec {
    private final boolean secondRollup;
    private final boolean countNulls;
    private final Object noKeyValue;
    private final Object nonUniqueValue;

    private static final class AggUniqueMemoKey implements AggregationMemoKey {
        private final boolean countNulls;
        private final Object noKeyValue;
        private final Object nonUniqueValue;

        private AggUniqueMemoKey(boolean countNulls, Object noKeyValue, Object nonUniqueValue) {
            this.countNulls = countNulls;
            this.noKeyValue = noKeyValue;
            this.nonUniqueValue = nonUniqueValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AggUniqueMemoKey that = (AggUniqueMemoKey) o;
            return countNulls == that.countNulls && Objects.equals(noKeyValue, that.noKeyValue)
                    && Objects.equals(nonUniqueValue, that.nonUniqueValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(countNulls, noKeyValue, nonUniqueValue);
        }
    }

    UniqueSpec() {
        this(false, false, null, null);
    }

    UniqueSpec(boolean countNulls) {
        this(false, countNulls, null, null);
    }

    UniqueSpec(boolean countNulls, Object noKeyValue, Object nonUniqueValue) {
        this(false, countNulls, noKeyValue, nonUniqueValue);
    }

    private UniqueSpec(boolean secondRollup, boolean countNulls, Object noKeyValue, Object nonUniqueValue) {
        this.secondRollup = secondRollup;
        this.countNulls = countNulls;
        this.noKeyValue = noKeyValue;
        this.nonUniqueValue = nonUniqueValue;
    }

    public Object getNoKeyValue() {
        return noKeyValue;
    }

    public Object getNonUniqueValue() {
        return nonUniqueValue;
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        return new AggUniqueMemoKey(countNulls, noKeyValue, nonUniqueValue);
    }

    @Override
    boolean supportsRollup() {
        return true;
    }

    @Override
    UniqueSpec forRollup() {
        return this;
    }

    @Override
    UniqueSpec rollupFactory() {
        return new UniqueSpec(true, countNulls, noKeyValue, nonUniqueValue);
    }

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return getUniqueChunked(type, name, countNulls, exposeInternalColumns, noKeyValue, nonUniqueValue,
                secondRollup);
    }

    public boolean countNulls() {
        return countNulls;
    }
}

package io.deephaven.db.v2.by;

import java.util.Objects;

/**
 * A Flavor of FirstBy that produces no values from the original table, only a named column of source keys.
 */
public class KeyOnlyFirstOrLastByStateFactory extends IterativeIndexStateFactory {

    private final String resultColumn;
    private final AggType type;

    private static class MemoKey implements AggregationMemoKey {
        private final String resultColumnName;
        private final AggType type;

        private MemoKey(String resultColumnName, AggType type) {
            this.resultColumnName = resultColumnName;
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MemoKey memoKey = (MemoKey) o;
            return Objects.equals(resultColumnName, memoKey.resultColumnName) && type == memoKey.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(resultColumnName, type);
        }
    }

    public KeyOnlyFirstOrLastByStateFactory(String resultColumn, AggType type) {
        super(false, false, 0);
        this.resultColumn = resultColumn;
        this.type = type;

        if (type != AggType.First && type != AggType.Last) {
            throw new IllegalArgumentException(
                    "KeyOnlyFirstOrLastByStateFactory only support AggType.First and AggType.Last");
        }
    }

    public String getResultColumn() {
        return resultColumn;
    }

    public boolean isLast() {
        return type == AggType.Last;
    }

    @Override
    boolean supportsRollup() {
        return false;
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        return new MemoKey(resultColumn, type);
    }

    @Override
    KeyOnlyFirstOrLastByStateFactory forRollup() {
        throw new UnsupportedOperationException("KeyOnlyFirstBy does not support rollups.");
    }

    @Override
    public String toString() {
        return "KeyOnlyFirstByStateFactory{resultColumn=" + resultColumn + '}';
    }
}

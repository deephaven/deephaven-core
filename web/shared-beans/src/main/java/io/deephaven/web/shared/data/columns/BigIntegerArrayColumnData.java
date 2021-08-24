package io.deephaven.web.shared.data.columns;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Holder for data associated with a column of type java.math.BigInteger.
 */
public class BigIntegerArrayColumnData extends ColumnData {
    private BigInteger[] data;

    public BigIntegerArrayColumnData() {}

    public BigIntegerArrayColumnData(BigInteger[] data) {
        this.data = data;
    }

    public BigInteger[] getData() {
        return data;
    }

    public void setData(BigInteger[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final BigIntegerArrayColumnData that = (BigIntegerArrayColumnData) o;
        return Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(data);
    }
}


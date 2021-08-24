package io.deephaven.web.shared.data.columns;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Holder for data associated with a column of type java.math.BigDecimal.
 */
public class BigDecimalArrayColumnData extends ColumnData {
    private BigDecimal[] data;

    public BigDecimalArrayColumnData() {}

    public BigDecimalArrayColumnData(BigDecimal[] data) {
        this.data = data;
    }

    public BigDecimal[] getData() {
        return data;
    }

    public void setData(BigDecimal[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final BigDecimalArrayColumnData that = (BigDecimalArrayColumnData) o;
        return Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(data);
    }
}


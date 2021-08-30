package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class DoubleArrayColumnData extends ColumnData {
    private double[] data;

    public DoubleArrayColumnData() {}

    public DoubleArrayColumnData(double[] data) {
        this.data = data;
    }

    public double[] getData() {
        return data;
    }

    public void setData(double[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DoubleArrayColumnData that = (DoubleArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

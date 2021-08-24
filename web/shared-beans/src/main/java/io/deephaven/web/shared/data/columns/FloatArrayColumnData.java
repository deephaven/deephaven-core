package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class FloatArrayColumnData extends ColumnData {
    private float[] data;

    public FloatArrayColumnData() {}

    public FloatArrayColumnData(float[] data) {
        this.data = data;
    }

    public float[] getData() {
        return data;
    }

    public void setData(float[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FloatArrayColumnData that = (FloatArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

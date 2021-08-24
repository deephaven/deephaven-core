package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class ShortArrayColumnData extends ColumnData {
    private short[] data;

    public ShortArrayColumnData() {}

    public ShortArrayColumnData(short[] data) {
        this.data = data;
    }

    public short[] getData() {
        return data;
    }

    public void setData(short[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ShortArrayColumnData that = (ShortArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class LongArrayColumnData extends ColumnData {
    private long[] data;

    public LongArrayColumnData() {}

    public LongArrayColumnData(long[] data) {
        this.data = data;
    }

    public long[] getData() {
        return data;
    }

    public void setData(long[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LongArrayColumnData that = (LongArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

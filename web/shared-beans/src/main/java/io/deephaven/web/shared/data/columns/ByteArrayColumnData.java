package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class ByteArrayColumnData extends ColumnData {
    private byte[] data;

    public ByteArrayColumnData() {}

    public ByteArrayColumnData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ByteArrayColumnData that = (ByteArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

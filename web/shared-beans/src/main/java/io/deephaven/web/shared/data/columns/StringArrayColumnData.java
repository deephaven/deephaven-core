package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class StringArrayColumnData extends ColumnData {
    private String[] data;

    public StringArrayColumnData() {}

    public StringArrayColumnData(String[] data) {
        this.data = data;
    }

    public String[] getData() {
        return data;
    }

    public void setData(String[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        StringArrayColumnData that = (StringArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

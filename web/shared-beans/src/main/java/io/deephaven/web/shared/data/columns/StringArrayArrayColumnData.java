package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class StringArrayArrayColumnData extends ColumnData {
    private String[][] data;

    public StringArrayArrayColumnData() {}

    public StringArrayArrayColumnData(String[][] data) {
        this.data = data;
    }

    public String[][] getData() {
        return data;
    }

    public void setData(String[][] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        StringArrayArrayColumnData that = (StringArrayArrayColumnData) o;

        return Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(data);
    }
}

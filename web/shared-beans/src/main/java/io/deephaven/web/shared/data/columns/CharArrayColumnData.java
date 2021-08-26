package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class CharArrayColumnData extends ColumnData {
    private char[] data;

    public CharArrayColumnData() {}

    public CharArrayColumnData(char[] data) {
        this.data = data;
    }

    public char[] getData() {
        return data;
    }

    public void setData(char[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CharArrayColumnData that = (CharArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

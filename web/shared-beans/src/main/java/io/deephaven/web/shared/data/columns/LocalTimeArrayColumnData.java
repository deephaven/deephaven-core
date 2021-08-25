package io.deephaven.web.shared.data.columns;

import io.deephaven.web.shared.data.LocalTime;

import java.util.Arrays;

/**
 * Holder for data associated with a column of type java.time.LocalTime. This type is serialized with a custom LocalTime
 * type for efficiency and GWT compatibility.
 */
public class LocalTimeArrayColumnData extends ColumnData {
    private LocalTime[] data;

    public LocalTimeArrayColumnData() {}

    public LocalTimeArrayColumnData(LocalTime[] data) {
        this.data = data;
    }

    public LocalTime[] getData() {
        return data;
    }

    public void setData(LocalTime[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final LocalTimeArrayColumnData that = (LocalTimeArrayColumnData) o;
        return Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(data);
    }
}

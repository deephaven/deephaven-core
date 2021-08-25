package io.deephaven.web.shared.data.columns;

import io.deephaven.web.shared.data.LocalDate;

import java.util.Arrays;

/**
 * Holder for data associated with a column of type java.time.LocalDate. This type is serialized
 * with a custom LocalDate type for efficiency and GWT compatibility.
 */
public class LocalDateArrayColumnData extends ColumnData {
    private LocalDate[] data;

    public LocalDateArrayColumnData() {}

    public LocalDateArrayColumnData(LocalDate[] data) {
        this.data = data;
    }

    public LocalDate[] getData() {
        return data;
    }

    public void setData(LocalDate[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final LocalDateArrayColumnData that = (LocalDateArrayColumnData) o;
        return Arrays.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(data);
    }
}


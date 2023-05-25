/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.data.columns;

import java.util.Arrays;

public class BooleanArrayColumnData extends ColumnData {
    private Boolean[] data;

    public BooleanArrayColumnData() {}

    public BooleanArrayColumnData(Boolean[] data) {
        this.data = data;
    }

    public Boolean[] getData() {
        return data;
    }

    public void setData(Boolean[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BooleanArrayColumnData that = (BooleanArrayColumnData) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

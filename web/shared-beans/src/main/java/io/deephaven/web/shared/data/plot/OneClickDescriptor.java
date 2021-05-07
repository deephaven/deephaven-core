package io.deephaven.web.shared.data.plot;

import java.io.Serializable;
import java.util.Arrays;

public class OneClickDescriptor implements Serializable {
    private String[] columns;
    private String[] columnTypes;
    private boolean requireAllFiltersToDisplay;

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(final String[] columns) {
        this.columns = columns;
    }

    public boolean isRequireAllFiltersToDisplay() {
        return requireAllFiltersToDisplay;
    }

    public void setRequireAllFiltersToDisplay(final boolean requireAllFiltersToDisplay) {
        this.requireAllFiltersToDisplay = requireAllFiltersToDisplay;
    }

    public String[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(final String[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final OneClickDescriptor that = (OneClickDescriptor) o;

        if (requireAllFiltersToDisplay != that.requireAllFiltersToDisplay) return false;
        if (!Arrays.equals(columns, that.columns)) return false;
        return Arrays.equals(columnTypes, that.columnTypes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(columns);
        result = 31 * result + Arrays.hashCode(columnTypes);
        result = 31 * result + (requireAllFiltersToDisplay ? 1 : 0);
        return result;
    }
}

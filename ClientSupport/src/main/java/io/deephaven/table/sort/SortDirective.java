package io.deephaven.table.sort;

import java.io.Serializable;

/**
 * An immutable directive to perform a sort on a column.
 */
public class SortDirective implements Serializable {

    private static final long serialVersionUID = 2L;

    public static final SortDirective EMPTY_DIRECTIVE =
        new SortDirective(null, SortDirective.NOT_SORTED, false);

    public static final int DESCENDING = -1;
    public static final int NOT_SORTED = 0;
    public static final int ASCENDING = 1;

    private final int direction;
    private final boolean isAbsolute;
    private final String columnName;

    public SortDirective(String columnName, int direction, boolean isAbsolute) {
        this.columnName = columnName;
        this.direction = direction;
        this.isAbsolute = isAbsolute;
    }

    public int getDirection() {
        return direction;
    }

    public boolean isAbsolute() {
        return isAbsolute;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return "SortDirective{" +
            "direction=" + direction +
            ", isAbsolute=" + isAbsolute +
            ", columnName='" + columnName + '\'' +
            '}';
    }
}

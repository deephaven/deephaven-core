package io.deephaven.web.client.api;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SortDescriptor;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

public class Sort {
    @JsProperty(namespace = "dh.Sort")
    public static final String ASCENDING = "ASC",
            DESCENDING = "DESC",
            REVERSE = "REVERSE";

    private static final Column REVERSE_COLUMN =
            new Column(-1, -1, null, null, "", "__REVERSE_COLUMN", false, null, null, false);

    private final Column column;
    private String direction;
    private boolean abs = false;

    public Sort(Column column) {
        this.column = column;
    }

    public static Sort reverse() {
        final Sort reverse = new Sort(REVERSE_COLUMN);
        reverse.direction = REVERSE;
        return reverse;
    }

    @JsProperty
    public Column getColumn() {
        return column;
    }

    @JsProperty
    public String getDirection() {
        return direction;
    }

    @JsProperty(name = "isAbs")
    public boolean isAbs() {
        return abs;
    }

    @JsMethod
    public Sort asc() {
        Sort sort = new Sort(column);
        sort.abs = abs;
        sort.direction = ASCENDING;
        return sort;
    }

    @JsMethod
    public Sort desc() {
        Sort sort = new Sort(column);
        sort.abs = abs;
        sort.direction = DESCENDING;
        return sort;
    }

    @JsMethod
    public Sort abs() {
        Sort sort = new Sort(column);
        sort.abs = true;
        sort.direction = direction;
        return sort;
    }

    public SortDescriptor makeDescriptor() {
        if (direction == null) {
            throw new IllegalStateException("Cannot perform a sort without a direction, please call desc() or asc()");
        }
        SortDescriptor descriptor = new SortDescriptor();
        descriptor.setIsAbsolute(isAbs());
        descriptor.setColumnName(getColumn().getName());
        switch (direction) {
            case ASCENDING:
                descriptor.setDirection(SortDescriptor.SortDirection.getASCENDING());
                break;
            case DESCENDING:
                descriptor.setDirection(SortDescriptor.SortDirection.getDESCENDING());
                break;
            case REVERSE:
                descriptor.setDirection(SortDescriptor.SortDirection.getREVERSE());
                break;
        }
        return descriptor;
    }

    @JsMethod
    @Override
    public String toString() {
        return "Sort{" +
                "column=" + column +
                ", direction='" + direction + '\'' +
                ", abs=" + abs +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final Sort sort = (Sort) o;

        if (abs != sort.abs)
            return false;
        if (!column.getName().equals(sort.column.getName()))
            return false;
        return direction.equals(sort.direction);
    }

    @Override
    public int hashCode() {
        int result = column.getName().hashCode();
        result = 31 * result + direction.hashCode();
        result = 31 * result + (abs ? 1 : 0);
        return result;
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SortDescriptor;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

/**
 * Describes a Sort present on the table. No visible constructor, created through the use of Column.sort(), will be tied
 * to that particular column data. Sort instances are immutable, and use a builder pattern to make modifications. All
 * methods return a new Sort instance.
 */
@TsName(namespace = "dh")
public class Sort {
    @JsProperty(namespace = "dh.Sort")
    public static final String ASCENDING = "ASC",
            DESCENDING = "DESC",
            REVERSE = "REVERSE";

    private static final Column REVERSE_COLUMN =
            new Column(-1, -1, null, null, "", "__REVERSE_COLUMN", false, null, null, false, false);

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

    /**
     * The column which is sorted.
     * 
     * @return {@link Column}
     */
    @JsProperty
    public Column getColumn() {
        return column;
    }

    /**
     * The direction of this sort, either <b>ASC</b>, <b>DESC</b>, or <b>REVERSE</b>.
     * 
     * @return String
     */
    @JsProperty
    public String getDirection() {
        return direction;
    }

    /**
     * True if the absolute value of the column should be used when sorting; defaults to false.
     * 
     * @return boolean
     */
    @JsProperty(name = "isAbs")
    public boolean isAbs() {
        return abs;
    }

    /**
     * Builds a Sort instance to sort values in ascending order.
     * 
     * @return {@link Sort}
     */
    @JsMethod
    public Sort asc() {
        Sort sort = new Sort(column);
        sort.abs = abs;
        sort.direction = ASCENDING;
        return sort;
    }

    /**
     * Builds a Sort instance to sort values in descending order.
     * 
     * @return {@link Sort}
     */
    @JsMethod
    public Sort desc() {
        Sort sort = new Sort(column);
        sort.abs = abs;
        sort.direction = DESCENDING;
        return sort;
    }

    /**
     * Builds a Sort instance which takes the absolute value before applying order.
     * 
     * @return {@link Sort}
     */
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

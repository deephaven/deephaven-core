//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Describes a {@link Sort} present on the table. No visible constructor, created through the use of
 * {@link Column#sort()}, will be tied to that particular column data. {@link Sort} instances are immutable, and use a
 * builder pattern to make modifications. All methods return a new {@link Sort} instance.
 */
@TsName(namespace = "dh")
public class Sort {
    @JsProperty(namespace = "dh.Sort")
    public static final String ASCENDING = "ASC",
            DESCENDING = "DESC",
            REVERSE = "REVERSE";

    /**
     * Simple structure to describe a sort.
     */
    @JsType(namespace = "dh", name = "SimpleSort")
    @TsInterface
    public static class SimpleSort {
        /**
         * The column or column name to sort.
         */
        Column.ColumnOrName column;
        /**
         * The direction to sort this column. If absent/null, sort is ascending.
         */
        @JsNullable
        String direction;

        /**
         * True to take the absolute value before sorting. Defaults to false if absent/null.
         */
        @JsNullable
        Boolean isAbs;
    }

    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    @TsUnion
    public interface SortUnion {
        @JsOverlay
        @TsUnionMember
        default Sort asSort() {
            return Js.uncheckedCast(this);
        }

        @JsOverlay
        @TsUnionMember
        default SimpleSort asSimpleSort() {
            return Js.uncheckedCast(this);
        }

        @JsOverlay
        @TsUnionMember
        default String asString() {
            return Js.uncheckedCast(this);
        }

        @JsOverlay
        @TsUnionMember
        default Column asColumn() {
            return Js.uncheckedCast(this);
        }

        @JsOverlay
        default String columnName() {
            if (Js.typeof(this).equals("string")) {
                return Js.uncheckedCast(this);
            }
            if (this instanceof Column c) {
                return c.getName();
            }
            return ((SimpleSort) this).column.columnName();
        }

        /**
         * Internal helper to build a proto sort descriptor from the variety of JS types.
         */
        @JsOverlay
        default SortDescriptor makeDescriptor() {
            if (this instanceof Sort s) {
                return s.makeDescriptor();
            } else if (Js.typeof(this).equals("string")) {
                return SortDescriptor.newBuilder()
                        .setColumnName(this.toString())
                        .build();
            } else if (this instanceof Column c) {
                return SortDescriptor.newBuilder()
                        .setColumnName(c.getName())
                        .build();
            } else {
                SimpleSort s = asSimpleSort();
                return SortDescriptor.newBuilder()
                        .setColumnName(s.column.columnName())
                        .setDirection(directionFromString(s.direction))
                        .setIsAbsolute(s.isAbs != null && s.isAbs)
                        .build();
            }
        }
    }

    private static final Column REVERSE_COLUMN =
            new Column(-1, -1, null, "", "__REVERSE_COLUMN", false, null, null, false, false, false, null);

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
     * The direction of this sort, either {@code ASC}, {@code DESC}, or {@code REVERSE}.
     * 
     * @return String
     */
    @JsProperty
    public String getDirection() {
        return direction;
    }

    /**
     * Set to {@code true} if the absolute value of the column should be used when sorting; defaults to {@code false}.
     * 
     * @return boolean
     */
    @JsProperty(name = "isAbs")
    public boolean isAbs() {
        return abs;
    }

    /**
     * Builds a {@link Sort} instance to sort values in ascending order.
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
     * Builds a {@link Sort} instance to sort values in descending order.
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
     * Builds a {@link Sort} instance which takes the absolute value before applying order.
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

    static SortDescriptor.SortDirection directionFromString(String direction) {
        if (direction == null) {
            return SortDescriptor.SortDirection.ASCENDING;
        }
        switch (direction) {
            case ASCENDING:
                return SortDescriptor.SortDirection.ASCENDING;
            case DESCENDING:
                return SortDescriptor.SortDirection.DESCENDING;
            case REVERSE:
                return SortDescriptor.SortDirection.REVERSE;
            default:
                throw new IllegalArgumentException("Unknown sort direction: " + direction);
        }
    }

    public SortDescriptor makeDescriptor() {
        if (direction == null) {
            throw new IllegalStateException("Cannot perform a sort without a direction, please call desc() or asc()");
        }
        SortDescriptor.Builder descriptor = SortDescriptor.newBuilder();
        descriptor.setIsAbsolute(isAbs());
        descriptor.setColumnName(getColumn().getName());
        descriptor.setDirection(directionFromString(direction));
        return descriptor.build();
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

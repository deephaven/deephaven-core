package io.deephaven.qst.table.column;

import io.deephaven.qst.AllowNulls;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.column.header.ColumnHeader;
import io.deephaven.qst.table.column.type.ColumnType;
import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Column<T> {

    public static <T> Column<T> empty(ColumnHeader<T> header) {
        return ImmutableColumn.<T>builder().header(header).build();
    }

    public static <T> Column<T> of(ColumnHeader<T> header, T... data) {
        return ImmutableColumn.<T>builder().header(header).addValues(data).build();
    }

    public static <T> Column<T> of(ColumnHeader<T> header, Iterable<T> data) {
        return ImmutableColumn.<T>builder().header(header).addAllValues(data).build();
    }

    public static <T> Column<T> of(String name, Class<T> clazz, Iterable<T> values) {
        return ColumnHeader.of(name, clazz).withData(values);
    }

    public static <T> Column<T> of(String name, Class<T> clazz, T... values) {
        return ColumnHeader.of(name, clazz).withData(values);
    }

    public static Column<Byte> of(String name, Byte... values) {
        return ColumnHeader.ofByte(name).withData(values);
    }

    public static Column<Character> of(String name, Character... values) {
        return ColumnHeader.ofChar(name).withData(values);
    }

    public static Column<Short> of(String name, Short... values) {
        return ColumnHeader.ofShort(name).withData(values);
    }

    public static Column<Integer> of(String name, Integer... values) {
        return ColumnHeader.ofInt(name).withData(values);
    }

    public static Column<Long> of(String name, Long... values) {
        return ColumnHeader.ofLong(name).withData(values);
    }

    public static Column<Float> of(String name, Float... values) {
        return ColumnHeader.ofFloat(name).withData(values);
    }

    public static Column<Double> of(String name, Double... values) {
        return ColumnHeader.ofDouble(name).withData(values);
    }

    public static Column<String> of(String name, String... values) {
        return ColumnHeader.ofString(name).withData(values);
    }

    public static <T> ColumnBuilder<T> builder(ColumnHeader<T> header) {
        return ImmutableColumn.<T>builder().header(header);
    }

    public static <T> Column<T> cast(@SuppressWarnings("unused") ColumnType<T> type, Column<?> column) {
        //noinspection unchecked
        return (Column<T>)column;
    }

    public abstract ColumnHeader<T> header();

    @AllowNulls
    public abstract List<T> values();

    public final String name() {
        return header().name();
    }

    public final ColumnType<T> type() {
        return header().type();
    }

    public final int size() {
        return values().size();
    }

    public final NewTable toTable() {
        return NewTable.of(this);
    }

    abstract static class Builder<T> implements ColumnBuilder<T> {
        @Override
        public final ColumnBuilder<T> add(T item) {
            return addValues(item);
        }

        abstract Builder<T> addValues(T element);
    }
}

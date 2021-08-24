package io.deephaven.qst.column;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.BooleanArray;
import io.deephaven.qst.array.ByteArray;
import io.deephaven.qst.array.CharArray;
import io.deephaven.qst.array.DoubleArray;
import io.deephaven.qst.array.FloatArray;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.ShortArray;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Instant;
import java.util.Collection;

/**
 * A column is {@link #name() name} and {@link #array() array} pair.
 *
 * @param <T> the array's item type
 */
@Immutable
@SimpleStyle
public abstract class Column<T> {

    public static <T> Column<T> of(String name, Array<T> array) {
        return ImmutableColumn.of(name, array);
    }

    public static <T> Column<T> empty(ColumnHeader<T> header) {
        return of(header.name(), Array.empty(header.componentType()));
    }

    public static <T> Column<T> of(ColumnHeader<T> header, T... data) {
        return of(header.name(), Array.of(header.componentType(), data));
    }

    public static <T> Column<T> of(ColumnHeader<T> header, Collection<T> data) {
        return of(header.name(), Array.of(header.componentType(), data));
    }

    public static <T> Column<T> of(String name, Class<T> clazz, T... values) {
        return of(name, Array.of(Type.find(clazz), values));
    }

    public static <T> Column<T> of(String name, Type<T> type, T... values) {
        return of(name, Array.of(type, values));
    }

    public static <T> Column<T> of(String name, Class<T> clazz, Collection<T> values) {
        return of(name, Array.of(Type.find(clazz), values));
    }

    public static Column<Boolean> of(String name, Boolean... values) {
        return of(name, BooleanArray.of(values));
    }

    public static Column<Byte> of(String name, Byte... values) {
        return of(name, ByteArray.of(values));
    }

    public static Column<Character> of(String name, Character... values) {
        return of(name, CharArray.of(values));
    }

    public static Column<Short> of(String name, Short... values) {
        return of(name, ShortArray.of(values));
    }

    public static Column<Integer> of(String name, Integer... values) {
        return of(name, IntArray.of(values));
    }

    public static Column<Long> of(String name, Long... values) {
        return of(name, LongArray.of(values));
    }

    public static Column<Float> of(String name, Float... values) {
        return of(name, FloatArray.of(values));
    }

    public static Column<Double> of(String name, Double... values) {
        return of(name, DoubleArray.of(values));
    }

    public static Column<String> of(String name, String... values) {
        return of(name, GenericArray.of(Type.stringType(), values));
    }

    public static Column<Instant> of(String name, Instant... values) {
        return of(name, GenericArray.of(Type.instantType(), values));
    }

    public static Column<Boolean> ofBoolean(String name, Boolean... values) {
        return of(name, BooleanArray.of(values));
    }

    public static Column<Byte> ofUnsafe(String name, byte[] values) {
        return of(name, ByteArray.ofUnsafe(values));
    }

    public static Column<Character> ofUnsafe(String name, char[] values) {
        return of(name, CharArray.ofUnsafe(values));
    }

    public static Column<Short> ofUnsafe(String name, short[] values) {
        return of(name, ShortArray.ofUnsafe(values));
    }

    public static Column<Integer> ofUnsafe(String name, int[] values) {
        return of(name, IntArray.ofUnsafe(values));
    }

    public static Column<Long> ofUnsafe(String name, long[] values) {
        return of(name, LongArray.ofUnsafe(values));
    }

    public static Column<Float> ofUnsafe(String name, float[] values) {
        return of(name, FloatArray.ofUnsafe(values));
    }

    public static Column<Double> ofUnsafe(String name, double[] values) {
        return of(name, DoubleArray.ofUnsafe(values));
    }

    public static Column<Byte> ofByte(String name, byte... values) {
        return of(name, ByteArray.of(values));
    }

    public static Column<Character> ofChar(String name, char... values) {
        return of(name, CharArray.of(values));
    }

    public static Column<Short> ofShort(String name, short... values) {
        return of(name, ShortArray.of(values));
    }

    public static Column<Integer> ofInt(String name, int... values) {
        return of(name, IntArray.of(values));
    }

    public static Column<Long> ofLong(String name, long... values) {
        return of(name, LongArray.of(values));
    }

    public static Column<Float> ofFloat(String name, float... values) {
        return of(name, FloatArray.of(values));
    }

    public static Column<Double> ofDouble(String name, double... values) {
        return of(name, DoubleArray.of(values));
    }

    public static <T> Column<T> cast(@SuppressWarnings("unused") Type<T> type, Column<?> column) {
        // noinspection unchecked
        return (Column<T>) column;
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract Array<T> array();

    public final ColumnHeader<T> header() {
        return ColumnHeader.of(name(), type());
    }

    public final Type<T> type() {
        return array().componentType();
    }

    public final int size() {
        return array().size();
    }

    public final NewTable toTable() {
        return NewTable.of(this);
    }
}

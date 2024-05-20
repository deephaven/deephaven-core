//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

/**
 * Utility methods for constructing {@link Vector Vectors} from the columns of a {@link Table}, enabling random and bulk
 * access to column data by row position.
 * <p>
 * Users should note that random access by row position (e.g. {@code get} methods) maybe be inefficient due to the need
 * to convert row positions to row keys and acquire implementation-specific resources once per row. Thus, random access
 * should generally be avoided when possible. On the other hand, bulk access methods (e.g. {@code iterator} and
 * {@code toArray} methods) are generally quite efficient because they can amortize row position to row key conversions
 * via iteration patterns and use bulk data movement operators.
 * <p>
 * Most usage of these APIs is perfectly safe, because script commands are run using an exclusive lock that inhibits
 * concurrent update processing, and table listeners run during a period when it is always safe to access previous and
 * current data for the table. That said, if the table is {@link Table#isRefreshing() refreshing}, it's important to
 * note that the returned vectors are only valid for use during the current cycle (or
 * {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase, when
 * {@code usePreviousValues = true}). See
 * <a href="https://deephaven.io/core/docs/conceptual/query-engine/engine-locking/">Engine Locking</a> for more
 * information on safe, consistent data access. {@link Vector#getDirect() Direct vectors} or {@link Vector#copyToArray()
 * copied arrays} are always safe to use if they are created while the vector is valid.
 */
public final class ColumnVectors {

    private ColumnVectors() {}

    /**
     * Get a {@link Vector} of the data belonging to the specified column.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     * <p>
     * Users should generally prefer one of the typed variants, e.g. {@link #ofChar ofChar} or {@link #ofObject
     * ofObject}, rather than this method.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return A {@link Vector} of the data belonging to the specified column
     */
    public static Vector<?> of(
            @NotNull final Table table,
            @NotNull final String columnName) {
        return of(table, columnName, false);
    }

    /**
     * Get a {@link Vector} of the data belonging to the specified column.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     * <p>
     * Users should generally prefer one of the typed variants, e.g. {@link #ofChar ofChar} or {@link #ofObject
     * ofObject}, rather than this method.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return A {@link Vector} of the data belonging to the specified column
     */
    public static Vector<?> of(
            @NotNull final Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table.getDefinition().checkHasColumn(columnName);
        final ColumnDefinition<?> columnDefinition = table.getDefinition().getColumn(columnName);
        final Class<?> type = columnDefinition.getDataType();
        if (type == char.class || type == Character.class) {
            return ofChar(table, columnName, usePreviousValues);
        }
        if (type == byte.class || type == Byte.class) {
            return ofByte(table, columnName, usePreviousValues);
        }
        if (type == short.class || type == Short.class) {
            return ofShort(table, columnName, usePreviousValues);
        }
        if (type == int.class || type == Integer.class) {
            return ofInt(table, columnName, usePreviousValues);
        }
        if (type == long.class || type == Long.class) {
            return ofLong(table, columnName, usePreviousValues);
        }
        if (type == float.class || type == Float.class) {
            return ofFloat(table, columnName, usePreviousValues);
        }
        if (type == double.class || type == Double.class) {
            return ofDouble(table, columnName, usePreviousValues);
        }
        return ofObject(table, columnName, type, usePreviousValues);
    }

    /**
     * Get a {@link CharVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code char}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return A {@link CharVector} of the data belonging to the specified column
     */
    public static CharVector ofChar(@NotNull final Table table, @NotNull final String columnName) {
        return ofChar(table, columnName, false);
    }

    /**
     * Get a {@link CharVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code char}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return A {@link CharVector} of the data belonging to the specified column
     */
    public static CharVector ofChar(
            @NotNull Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<Character> columnSource = table.getColumnSource(columnName, char.class);
        return usePreviousValues && table.isRefreshing()
                ? new CharVectorColumnWrapper(columnSource.getPrevSource(), rowSet.prev())
                : new CharVectorColumnWrapper(columnSource, rowSet);
    }

    /**
     * Get a {@link ByteVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code byte}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return A {@link ByteVector} of the data belonging to the specified column
     */
    public static ByteVector ofByte(@NotNull final Table table, @NotNull final String columnName) {
        return ofByte(table, columnName, false);
    }

    /**
     * Get a {@link ByteVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code byte}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return A {@link ByteVector} of the data belonging to the specified column
     */
    public static ByteVector ofByte(
            @NotNull Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<Byte> columnSource = table.getColumnSource(columnName, byte.class);
        return usePreviousValues && table.isRefreshing()
                ? new ByteVectorColumnWrapper(columnSource.getPrevSource(), rowSet.prev())
                : new ByteVectorColumnWrapper(columnSource, rowSet);
    }

    /**
     * Get a {@link ShortVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code short}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return A {@link ShortVector} of the data belonging to the specified column
     */
    public static ShortVector ofShort(@NotNull final Table table, @NotNull final String columnName) {
        return ofShort(table, columnName, false);
    }

    /**
     * Get a {@link ShortVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code short}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return A {@link ShortVector} of the data belonging to the specified column
     */
    public static ShortVector ofShort(
            @NotNull Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<Short> columnSource = table.getColumnSource(columnName, short.class);
        return usePreviousValues && table.isRefreshing()
                ? new ShortVectorColumnWrapper(columnSource.getPrevSource(), rowSet.prev())
                : new ShortVectorColumnWrapper(columnSource, rowSet);
    }

    /**
     * Get an {@link IntVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code int}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return An {@link IntVector} of the data belonging to the specified column
     */
    public static IntVector ofInt(@NotNull final Table table, @NotNull final String columnName) {
        return ofInt(table, columnName, false);
    }

    /**
     * Get an {@link IntVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code int}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return An {@link IntVector} of the data belonging to the specified column
     */
    public static IntVector ofInt(
            @NotNull Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<Integer> columnSource = table.getColumnSource(columnName, int.class);
        return usePreviousValues && table.isRefreshing()
                ? new IntVectorColumnWrapper(columnSource.getPrevSource(), rowSet.prev())
                : new IntVectorColumnWrapper(columnSource, rowSet);
    }

    /**
     * Get a {@link LongVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code long}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return A {@link LongVector} of the data belonging to the specified column
     */
    public static LongVector ofLong(@NotNull final Table table, @NotNull final String columnName) {
        return ofLong(table, columnName, false);
    }

    /**
     * Get a {@link LongVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code long}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return A {@link LongVector} of the data belonging to the specified column
     */
    public static LongVector ofLong(
            @NotNull Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<Long> columnSource = table.getColumnSource(columnName, long.class);
        return usePreviousValues && table.isRefreshing()
                ? new LongVectorColumnWrapper(columnSource.getPrevSource(), rowSet.prev())
                : new LongVectorColumnWrapper(columnSource, rowSet);
    }

    /**
     * Get a {@link FloatVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code float}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return A {@link FloatVector} of the data belonging to the specified column
     */
    public static FloatVector ofFloat(@NotNull final Table table, @NotNull final String columnName) {
        return ofFloat(table, columnName, false);
    }

    /**
     * Get a {@link FloatVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code float}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return A {@link FloatVector} of the data belonging to the specified column
     */
    public static FloatVector ofFloat(
            @NotNull Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<Float> columnSource = table.getColumnSource(columnName, float.class);
        return usePreviousValues && table.isRefreshing()
                ? new FloatVectorColumnWrapper(columnSource.getPrevSource(), rowSet.prev())
                : new FloatVectorColumnWrapper(columnSource, rowSet);
    }

    /**
     * Get a {@link DoubleVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code double}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @return A {@link DoubleVector} of the data belonging to the specified column
     */
    public static DoubleVector ofDouble(@NotNull final Table table, @NotNull final String columnName) {
        return ofDouble(table, columnName, false);
    }

    /**
     * Get a {@link DoubleVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code double}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return A {@link DoubleVector} of the data belonging to the specified column
     */
    public static DoubleVector ofDouble(
            @NotNull Table table,
            @NotNull final String columnName,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<Double> columnSource = table.getColumnSource(columnName, double.class);
        return usePreviousValues && table.isRefreshing()
                ? new DoubleVectorColumnWrapper(columnSource.getPrevSource(), rowSet.prev())
                : new DoubleVectorColumnWrapper(columnSource, rowSet);
    }

    /**
     * Get an {@link ObjectVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code DATA_TYPE}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param type The data type of the column
     * @return An {@link ObjectVector} of the data belonging to the specified column
     */
    public static <DATA_TYPE> ObjectVector<DATA_TYPE> ofObject(
            @NotNull final Table table,
            @NotNull final String columnName,
            @NotNull final Class<DATA_TYPE> type) {
        return ofObject(table, columnName, type, false);
    }

    /**
     * Get an {@link ObjectVector} of the data belonging to the specified column, which must be of
     * {@link ColumnSource#getType() type} {@code DATA_TYPE}.
     * <p>
     * See {@link ColumnVectors class-level documentation} for more details on recommended usage and safety.
     *
     * @param table The {@link Table} to access column data from
     * @param columnName The name of the column to access
     * @param type The data type of the column
     * @param usePreviousValues Whether the resulting vector should contain the previous values of the column. This is
     *        only meaningful if {@code table} is {@link Table#isRefreshing() refreshing}, and only defined during the
     *        {@link io.deephaven.engine.updategraph.LogicalClock.State#Updating Updating} phase of a cycle that isn't
     *        the instantiation cycle of {@code table}.
     * @return An {@link ObjectVector} of the data belonging to the specified column
     */
    public static <DATA_TYPE> ObjectVector<DATA_TYPE> ofObject(
            @NotNull Table table,
            @NotNull final String columnName,
            @NotNull final Class<DATA_TYPE> type,
            final boolean usePreviousValues) {
        table = table.coalesce();
        final TrackingRowSet rowSet = table.getRowSet();
        final ColumnSource<DATA_TYPE> columnSource = table.getColumnSource(columnName, type);
        return usePreviousValues && table.isRefreshing()
                ? new ObjectVectorColumnWrapper<>(columnSource.getPrevSource(), rowSet.prev())
                : new ObjectVectorColumnWrapper<>(columnSource, rowSet);
    }
}

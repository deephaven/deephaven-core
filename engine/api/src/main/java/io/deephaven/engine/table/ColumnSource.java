//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * A "source" for column data - allows cell values to be looked up by (long) keys.
 *
 * <p>
 * Note for implementors: All {@link ColumnSource} implementations must map {@link RowSet#NULL_ROW_KEY} to a null value
 * for all {@code get} and {@code getPrev} methods.
 */
public interface ColumnSource<T>
        extends ChunkSource.WithPrev<Values>, ElementSource<T>, TupleSource<T>, Releasable {

    ColumnSource[] ZERO_LENGTH_COLUMN_SOURCE_ARRAY = new ColumnSource[0];

    Class<T> getType();

    Class<?> getComponentType();

    @FinalDefault
    default ChunkType getChunkType() {
        final Class<T> dataType = getType();
        if (dataType == Boolean.class) {
            return ChunkType.Object;
        }
        return ChunkType.fromElementType(dataType);
    }

    /**
     * Return a {@link RowSet row set} where the values in the column source match the given keys.
     *
     * @param invertMatch Whether to invert the match, i.e. return the rows where the values do not match the given keys
     * @param usePrev Whether to use the previous values for the ColumnSource
     * @param caseInsensitive Whether to perform a case insensitive match
     * @param dataIndex An optional data index that can be used to accelerate the match (the index table must be
     *        included in snapshot controls or otherwise guaranteed to be current)
     * @param mapper Restrict results to this row set
     * @param keys The keys to match in the column
     *
     * @return The rows that match the given keys
     */
    WritableRowSet match(
            boolean invertMatch,
            boolean usePrev,
            boolean caseInsensitive,
            @Nullable final DataIndex dataIndex,
            @NotNull RowSet mapper,
            Object... keys);

    /**
     * ColumnSource implementations that track previous values have the option to not actually start tracking previous
     * values until this method is called. This is an option, not an obligation: some simple ColumnSource
     * implementations (like TSingleValueSource for various T) always track previous values; other implementations (like
     * PrevColumnSource) never do; some (like TArrayColumnSource) only start tracking once this method is called.
     *
     * An immutable column source can not have distinct prev values; therefore it is implemented as a no-op.
     */
    default void startTrackingPrevValues() {
        if (!isImmutable()) {
            throw new UnsupportedOperationException(this.getClass().getName());
        }
    }

    /**
     * Determine if this column source is immutable, meaning that the values at a given row key never change.
     *
     * @return true if the values at a given row key of the column source never change, false otherwise
     */
    boolean isImmutable();

    /**
     * Release any resources held for caching purposes. Implementations need not guarantee that concurrent accesses are
     * correct, as the purpose of this method is to ensure cleanup for column sources that will no longer be used.
     */
    @Override
    @OverridingMethodsMustInvokeSuper
    default void releaseCachedResources() {
        Releasable.super.releaseCachedResources();
    }

    /**
     * Test if a reinterpret call will succeed.
     *
     * @param alternateDataType The alternative type to consider
     * @return If a reinterpret on this column source with the supplied alternateDataType will succeed.
     */
    <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType);

    /**
     * Provide an alternative view into the data underlying this column source.
     *
     * @param alternateDataType The alternative type to expose
     * @return A column source of the alternate data type, backed by the same underlying data.
     * @throws IllegalArgumentException If the alternativeDataType supplied is not supported
     */
    <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> reinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException;

    @Override
    default T createTuple(final long rowKey) {
        return get(rowKey);
    }

    @Override
    default T createPreviousTuple(final long rowKey) {
        return getPrev(rowKey);
    }

    @Override
    default T createTupleFromValues(@NotNull final Object... values) {
        // noinspection unchecked
        return (T) values[0];
    }

    @Override
    @FinalDefault
    default int tupleLength() {
        return 1;
    }

    @Override
    @FinalDefault
    default <ELEMENT_TYPE> void exportElement(@NotNull final T tuple, final int elementIndex,
            @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        // noinspection unchecked
        writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple);
    }

    @Override
    @FinalDefault
    default Object exportElement(@NotNull final T tuple, final int elementIndex) {
        Require.eqZero(elementIndex, "elementIndex");
        return tuple;
    }

    @Override
    @FinalDefault
    default void exportAllTo(final Object @NotNull [] dest, @NotNull final T tuple) {
        Require.geqZero(dest.length, "dest.length");
        dest[0] = tuple;
    }

    @Override
    ColumnSource<T> getPrevSource();

    /**
     * Returns this {@code ColumnSource}, parameterized by {@code <TYPE>}, if the data type of this column (as given by
     * {@link #getType()}) can be cast to {@code clazz}. This is analogous to casting the objects provided by this
     * column source to {@code clazz}.
     * <p>
     * For example, the following code will throw an exception if the "MyString" column does not actually contain
     * {@code String} data:
     *
     * <pre>
     *     ColumnSource&lt;String&gt; colSource = table.getColumnSource("MyString").cast(String.class)
     * </pre>
     * <p>
     * Due to the nature of type erasure, the JVM will still insert an additional cast to {@code TYPE} when elements are
     * retrieved from the column source, such as with {@code String myStr = colSource.get(0)}.
     *
     * @param clazz The target type.
     * @param <TYPE> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return A {@code ColumnSource} parameterized by {@code TYPE}.
     */
    @FinalDefault
    default <TYPE> ColumnSource<TYPE> cast(Class<? extends TYPE> clazz) {
        return cast(clazz, (String) null);
    }

    /**
     * Returns this {@code ColumnSource}, parameterized by {@code <TYPE>}, if the data type of this column (as given by
     * {@link #getType()}) can be cast to {@code clazz}. This is analogous to casting the objects provided by this
     * column source to {@code clazz}.
     * <p>
     * For example, the following code will throw an exception if the "MyString" column does not actually contain
     * {@code String} data:
     *
     * <pre>
     *     ColumnSource&lt;String&gt; colSource = table.getColumnSource("MyString").cast(String.class, "MyString")
     * </pre>
     * <p>
     * Due to the nature of type erasure, the JVM will still insert an additional cast to {@code TYPE} when elements are
     * retrieved from the column source, such as with {@code String myStr = colSource.get(0)}.
     *
     * @param clazz The target type.
     * @param <TYPE> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @param colName An optional column name, which will be included in exception messages.
     * @return A {@code ColumnSource} parameterized by {@code TYPE}.
     */
    @FinalDefault
    default <TYPE> ColumnSource<TYPE> cast(Class<? extends TYPE> clazz, @Nullable String colName) {
        Require.neqNull(clazz, "clazz");
        final String castCheckPrefix = colName == null ? "ColumnSource" : "ColumnSource[" + colName + ']';
        TypeHelper.checkCastTo(castCheckPrefix, getType(), clazz);
        // noinspection unchecked
        return (ColumnSource<TYPE>) this;
    }

    /**
     * Returns this {@code ColumnSource}, parameterized by {@code <TYPE>}, if the data type of this column (as given by
     * {@link #getType()}) can be cast to {@code clazz}. This is analogous to casting the objects provided by this
     * column source to {@code clazz}. Additionally, this checks that the component type of this column (as given by
     * {@link #getComponentType()}) can be cast to {@code componentType} (both must be present and castable, or both
     * must be {@code null}).
     *
     * <p>
     * For example, the following code will throw an exception if the "MyString" column does not actually contain
     * {@code String} data:
     *
     * <pre>
     *     ColumnSource&lt;String&gt; colSource = table.getColumnSource("MyString").cast(String.class, null)
     * </pre>
     * <p>
     * Due to the nature of type erasure, the JVM will still insert an additional cast to {@code TYPE} when elements are
     * retrieved from the column source, such as with {@code String myStr = colSource.get(0)}.
     *
     * @param clazz The target type.
     * @param componentType The target component type, may be {@code null}.
     * @param <TYPE> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return A {@code ColumnSource} parameterized by {@code TYPE}.
     */
    @FinalDefault
    default <TYPE> ColumnSource<TYPE> cast(Class<? extends TYPE> clazz, @Nullable Class<?> componentType) {
        return cast(clazz, componentType, null);
    }

    /**
     * Returns this {@code ColumnSource}, parameterized by {@code <TYPE>}, if the data type of this column (as given by
     * {@link #getType()}) can be cast to {@code clazz}. This is analogous to casting the objects provided by this
     * column source to {@code clazz}. Additionally, this checks that the component type of this column (as given by
     * {@link #getComponentType()}) can be cast to {@code componentType} (both must be present and castable, or both
     * must be {@code null}).
     *
     * <p>
     * For example, the following code will throw an exception if the "MyString" column does not actually contain
     * {@code String} data:
     *
     * <pre>
     *     ColumnSource&lt;String&gt; colSource = table.getColumnSource("MyString").cast(String.class, null, "MyString")
     * </pre>
     * <p>
     * Due to the nature of type erasure, the JVM will still insert an additional cast to {@code TYPE} when elements are
     * retrieved from the column source, such as with {@code String myStr = colSource.get(0)}.
     *
     * @param clazz The target type.
     * @param componentType The target component type, may be {@code null}.
     * @param colName An optional column name, which will be included in exception messages.
     * @param <TYPE> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return A {@code ColumnSource} parameterized by {@code TYPE}.
     */
    @FinalDefault
    default <TYPE> ColumnSource<TYPE> cast(Class<? extends TYPE> clazz, @Nullable Class<?> componentType,
            @Nullable String colName) {
        Require.neqNull(clazz, "clazz");
        final String castCheckPrefix = colName == null ? "ColumnSource" : "ColumnSource[" + colName + ']';
        TypeHelper.checkCastTo(castCheckPrefix, getType(), getComponentType(), clazz, componentType);
        // noinspection unchecked
        return (ColumnSource<TYPE>) this;
    }

    /**
     * Most column sources will return the same value for a given row without respect to the order that the rows are
     * read. Those columns sources are considered "stateless" and should return true.
     *
     * Some column sources, however may be dependent on evaluation order. For example, a formula that updates a Map must
     * be evaluated from the first row to the last row. A column source that has the potential to depend on the order of
     * evaluation must return false.
     *
     * @return true if this is a stateless column source
     */
    default boolean isStateless() {
        return true;
    }
}

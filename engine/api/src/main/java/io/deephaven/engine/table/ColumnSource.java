/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, RowSet mapper,
            final Object... keys);

    Map<T, RowSet> getValuesMapping(RowSet subRange);

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
     * Compute grouping information for all keys present in this column source.
     *
     * @return A map from distinct data values to a RowSet that contains those values
     */
    Map<T, RowSet> getGroupToRange();

    /**
     * Compute grouping information for (at least) all keys present in rowSet.
     *
     * @param rowSet The RowSet to consider
     * @return A map from distinct data values to a RowSet that contains those values
     */
    Map<T, RowSet> getGroupToRange(RowSet rowSet);

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
    default <ELEMENT_TYPE> void exportElement(final T tuple, final int elementIndex,
            @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        // noinspection unchecked
        writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple);
    }

    @Override
    default Object exportElement(T tuple, int elementIndex) {
        Require.eqZero(elementIndex, "elementIndex");
        return tuple;
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
     *     ColumnSource&lt;String&gt; colSource = table.getColumnSource("MyString").getParameterized(String.class)
     * </pre>
     * <p>
     * Due to the nature of type erasure, the JVM will still insert an additional cast to {@code TYPE} when elements are
     * retrieved from the column source, such as with {@code String myStr = colSource.get(0)}.
     *
     * @param clazz The target type.
     * @param <TYPE> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return A {@code ColumnSource} parameterized by {@code TYPE}.
     */
    default <TYPE> ColumnSource<TYPE> cast(Class<? extends TYPE> clazz) {
        Require.neqNull(clazz, "clazz");
        final Class<?> columnSourceType = getType();
        if (!clazz.isAssignableFrom(columnSourceType)) {
            throw new ClassCastException(
                    "Cannot convert column source for type " + columnSourceType.getName() + " to " +
                            "type " + clazz.getName());
        }
        // noinspection unchecked
        return (ColumnSource<TYPE>) this;
    }

    /**
     * Can this column source be evaluated on an arbitrary thread?
     *
     * Most column sources can be evaluated on an arbitrary thread, however those that do call into Python can not be
     * evaluated on an arbitrary thread as the calling thread may already have the GIL, which would result in a deadlock
     * when the column source takes the GIL to evaluate formulas.
     *
     * @return true if this column prevents parallelization
     */
    default boolean preventsParallelism() {
        return false;
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

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.db.v2.utils.Index;
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
 * Note for implementors: All {@link ColumnSource} implementations must map {@link Index#NULL_KEY}
 * to a null value for all {@code get} and {@code getPrev} methods.
 */
public interface ColumnSource<T>
    extends DefaultChunkSource.WithPrev<Values>, ElementSource<T>, TupleSource<T>, Releasable {

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

    Index match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, Index mapper,
        final Object... keys);

    Map<T, Index> getValuesMapping(Index subRange);

    /**
     * ColumnSource implementations that track previous values have the option to not actually start
     * tracking previous values until this method is called. This is an option, not an obligation:
     * some simple ColumnSource implementations (like TSingleValueSource for various T) always track
     * previous values; other implementations (like PrevColumnSource) never do; some (like
     * TArrayColumnSource) only start tracking once this method is called.
     *
     * An immutable column source can not have distinct prev values; therefore it is implemented as
     * a no-op.
     */
    default void startTrackingPrevValues() {
        if (!isImmutable()) {
            throw new UnsupportedOperationException(this.getClass().getName());
        }
    }

    /**
     * Compute grouping information for all keys present in this column source.
     *
     * @return A map from distinct data values to an index that contains those values
     */
    Map<T, Index> getGroupToRange();

    /**
     * Compute grouping information for (at least) all keys present in index.
     *
     * @param index The index to consider
     * @return A map from distinct data values to an index that contains those values
     */
    Map<T, Index> getGroupToRange(Index index);

    /**
     * Determine if this column source is immutable, meaning that the values at a given index key
     * never change.
     *
     * @return true if the values at a given index of the column source never change, false
     *         otherwise
     */
    boolean isImmutable();

    /**
     * Release any resources held for caching purposes. Implementations need not guarantee that
     * concurrent accesses are correct, as the purpose of this method is to ensure cleanup for
     * column sources that will no longer be used.
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
     * @return If a reinterpret on this column source with the supplied alternateDataType will
     *         succeed.
     */
    <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
        @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType);

    /**
     * Provide an alternative view into the data underlying this column source.
     *
     * @param alternateDataType The alternative type to expose
     * @return A column source of the alternate data type, backed by the same underlying data.
     * @throws IllegalArgumentException If the alternativeDataType supplied is not supported
     */
    <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> reinterpret(
        @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType)
        throws IllegalArgumentException;

    @Override
    default List<ColumnSource> getColumnSources() {
        return Collections.singletonList(this);
    }

    @Override
    default T createTuple(final long indexKey) {
        return get(indexKey);
    }

    @Override
    default T createPreviousTuple(final long indexKey) {
        return getPrev(indexKey);
    }

    @Override
    default T createTupleFromValues(@NotNull final Object... values) {
        // noinspection unchecked
        return (T) values[0];
    }

    @Override
    default <ELEMENT_TYPE> void exportElement(final T tuple, final int elementIndex,
        @NotNull final WritableSource<ELEMENT_TYPE> writableSource,
        final long destinationIndexKey) {
        // noinspection unchecked
        writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple);
    }

    @Override
    default Object exportElement(T tuple, int elementIndex) {
        Require.eqZero(elementIndex, "elementIndex");
        return tuple;
    }

    @Override
    default Object exportToExternalKey(T tuple) {
        return tuple;
    }

    @Override
    default ChunkSource<Attributes.Values> getPrevSource() {
        return new PrevColumnSource<>(this);
    }

    /**
     * Returns this {@code ColumnSource}, parameterized by {@code <TYPE>}, if the data type of this
     * column (as given by {@link #getType()}) can be cast to {@code clazz}. This is analogous to
     * casting the objects provided by this column source to {@code clazz}.
     * <p>
     * For example, the following code will throw an exception if the "MyString" column does not
     * actually contain {@code String} data:
     *
     * <pre>
     *     ColumnSource&lt;String&gt; colSource = table.getColumnSource("MyString").getParameterized(String.class)
     * </pre>
     * <p>
     * Due to the nature of type erasure, the JVM will still insert an additional cast to
     * {@code TYPE} when elements are retrieved from the column source, such as with
     * {@code String myStr = colSource.get(0)}.
     *
     * @param clazz The target type.
     * @param <TYPE> The target type, as a type parameter. Intended to be inferred from
     *        {@code clazz}.
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
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * A transformation to apply to an existing {@link DataIndex data index} in order to produce a transformed
 * {@link BasicDataIndex}.
 */
@Immutable
@BuildableStyle
public interface DataIndexTransformer {

    /**
     * A {@link RowSet} to {@link RowSet#intersect(RowSet) intersect} with input RowSets when producing output RowSets.
     * If present, the result {@link BasicDataIndex} will be a static snapshot. This is the first transformation applied
     * if present.
     */
    Optional<RowSet> intersectRowSet();

    /*
     * A {@link RowSet} to {@link RowSet#invert(RowSet) invert} the input RowSets with when producing output RowSets. If
     * present, the result {@link BasicDataIndex} will be a static snapshot. This is always applied after {@link
     * #intersectRowSet() if present.
     */
    Optional<RowSet> invertRowSet();

    /**
     * Whether to sort the output {@link BasicDataIndex BasicDataIndex's} {@link BasicDataIndex#table() table} by the
     * first row key in each output {@link RowSet}. This is always applied after {@link #intersectRowSet()} and
     * {@link #invertRowSet()} if present. Note that when sorting a {@link BasicDataIndex#isRefreshing() refreshing}
     * index, operations that rely on the transformed index must be sure to depend on the <em>transformed</em> index,
     * and not the input index, for correct satisfaction.
     */
    @Default
    default boolean sortByFirstRowKey() {
        return false;
    }

    /**
     * @return Whether the set of transformations will force the result index table to be a static snapshot.
     */
    @FinalDefault
    default boolean snapshotResult() {
        return intersectRowSet().isPresent() || invertRowSet().isPresent();
    }

    @Check
    default void checkNotEmpty() {
        if (intersectRowSet().isEmpty() && invertRowSet().isEmpty() && !sortByFirstRowKey()) {
            throw new IllegalArgumentException("DataIndexTransformer must specify at least one transformation");
        }
    }

    /**
     * Create a {@link DataIndexTransformer.Builder builder} that specifies transformations to apply to an existing
     * {@link DataIndex data index}.
     * <p>
     * When multiple transformations are specified, they are applied in a specific order:
     * <ol>
     * <li>Intersect the index {@link RowSet RowSets} with the supplied RowSet. Note that the result will always be a
     * static snapshot.</li>
     * <li>Invert the index {@link RowSet RowSets} with the supplied RowSet. Note that the result will always be a
     * static snapshot.</li>
     * <li>Sort the index table by the first row key within each {@link RowSet}. Be careful to use the correct
     * dependency for satisfaction!</li>
     * </ol>
     * </p>
     *
     * @return A new {@link DataIndexTransformer} builder.
     */
    static Builder builder() {
        return ImmutableDataIndexTransformer.builder();
    }

    interface Builder {

        /**
         * Intersect the index {@link RowSet RowSets} with {@code rowSet}. All
         * {@link DataIndex#transform(DataIndexTransformer) transformations} using the resulting DataIndexTransformer
         * should be {@link BasicDataIndex#table() materialized} before {@code rowSet} is {@link RowSet#close() closed}.
         * The result {@link BasicDataIndex} will be a static snapshot.
         */
        Builder intersectRowSet(RowSet rowSet);

        /**
         * Invert the index {@link RowSet RowSets} with the supplied RowSet. All
         * {@link DataIndex#transform(DataIndexTransformer) transformations} using the resulting DataIndexTransformer
         * should be {@link BasicDataIndex#table() materialized} before {@code rowSet} is {@link RowSet#close() closed}.
         * The result {@link BasicDataIndex} will be a static snapshot.
         */
        Builder invertRowSet(RowSet rowSet);

        /**
         * Whether to sort the index table by the first row key within each {@link RowSet}. Defaults to {@code false}.
         * Be careful to use the correct dependency for satisfaction!
         */
        @SuppressWarnings("unused")
        Builder sortByFirstRowKey(boolean sort);

        DataIndexTransformer build();
    }
}

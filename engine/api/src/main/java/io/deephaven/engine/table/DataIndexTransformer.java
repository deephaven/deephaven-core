package io.deephaven.engine.table;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.rowset.RowSet;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Map;
import java.util.Optional;

@Immutable
@BuildableStyle
public interface DataIndexTransformer {

    /** The row set to {@link RowSet#intersect(RowSet)} the output row sets. */
    Optional<RowSet> intersectRowSet();

    /** The row set to {@link RowSet#invert(RowSet)} the output row sets. */
    Optional<RowSet> invertRowSet();

    /** Whether to sort the output table by the first row key in each output row set. */
    @Value.Default
    default boolean sortByFirstRowKey() {
        return false;
    }

    Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap();

    @Value.Default
    default boolean immutable() {
        return false;
    }

    static Builder builder() {
        return ImmutableDataIndexTransformer.builder();
    }

    interface Builder {

        /** Intersect the output row sets with the provided {@link RowSet}. */
        Builder intersectRowSet(RowSet rowSet);

        /** Invert the output row sets with the provided {@link RowSet}. */
        Builder invertRowSet(RowSet rowSet);

        /** Whether to sort the index table by the first row key within each row set. */
        @SuppressWarnings("unused")
        Builder sortByFirstRowKey(boolean sort);

        /** Map the new key columns to the old columns. */
        @SuppressWarnings("unused")
        Builder putOldToNewColumnMap(ColumnSource<?> key, ColumnSource<?> value);

        /** Map the new key columns to the old columns. */
        @SuppressWarnings("unused")
        Builder putOldToNewColumnMap(Map.Entry<? extends ColumnSource<?>, ? extends ColumnSource<?>> entry);

        /** Map the new key columns to the old columns. */
        Builder putAllOldToNewColumnMap(Map<? extends ColumnSource<?>, ? extends ColumnSource<?>> entries);

        /** Whether to force the materialized table to be static and immutable. */
        Builder immutable(boolean immutable);

        DataIndexTransformer build();
    }
}

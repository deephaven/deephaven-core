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

    Optional<RowSet> intersectRowSet();

    Optional<RowSet> invertRowSet();

    @Value.Default
    default boolean sortByFirstRowKey() {
        return false;
    }

    Map<ColumnSource<?>, ColumnSource<?>> keyColumnRemap();

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

        /** Sort the index table by the first key within each row set. */
        @SuppressWarnings("unused")
        Builder sortByFirstRowKey(boolean sort);

        /** Remap the new key columns to the old columns. */
        @SuppressWarnings("unused")
        Builder putKeyColumnRemap(ColumnSource<?> key, ColumnSource<?> value);

        @SuppressWarnings("unused")
        Builder putKeyColumnRemap(Map.Entry<? extends ColumnSource<?>, ? extends ColumnSource<?>> entry);

        Builder putAllKeyColumnRemap(Map<? extends ColumnSource<?>, ? extends ColumnSource<?>> entries);

        Builder immutable(boolean immutable);

        DataIndexTransformer build();
    }
}

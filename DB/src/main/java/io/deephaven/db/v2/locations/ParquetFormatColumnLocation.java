package io.deephaven.db.v2.locations;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.Nullable;

import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnLocation} sub-interface for column locations stored in the Apache Parquet columnar format.
 */
public interface ParquetFormatColumnLocation<ATTR extends Attributes.Any, TLT extends TableLocation> extends ColumnLocation<TLT> {

    /**
     * Get the {@link ColumnChunkPageStore} backing this column location.
     *
     * @return The page store
     */
    @NotNull
    ColumnChunkPageStore<ATTR> getPageStore(ColumnDefinition columnDefinition);

    /**
     * Get the {@link Dictionary} backing this column location.
     *
     * @return The dictionary, or null if it doesn't exist
     */
    @Nullable
    Chunk<ATTR> getDictionary(ColumnDefinition columnDefinition);

    /**
     * Get the {@link ColumnChunkPageStore} backing the indices for this column location. Only usable when there's
     * a dictionary.
     *
     * @return The page store
     */
    @Nullable
    ColumnChunkPageStore<Attributes.DictionaryKeys> getDictionaryKeysPageStore(ColumnDefinition columnDefinition);
}

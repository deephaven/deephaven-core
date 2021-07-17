/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.local;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.AbstractColumnLocation;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.regioned.*;
import io.deephaven.db.v2.utils.ChunkBoxer;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.deephaven.db.v2.sources.regioned.RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK;

final class ParquetColumnLocation<ATTR extends Any> extends AbstractColumnLocation {

    private static final int CHUNK_SIZE = Configuration.getInstance().getIntegerForClassWithDefault(ParquetColumnLocation.class, "chunkSize", 4096);

    /**
     * Factory object needed for deferred initialization of the remaining fields. Reference serves as a barrier to ensure
     * visibility of the derived fields.
     */
    private volatile ColumnChunkPageStore.Creator<ATTR> pageStoreCreator;

    private ColumnChunkPageStore<ATTR> pageStore;
    private Chunk<ATTR> dictionary;
    private ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore;
    private Supplier<?> getMetadata;

    /**
     * Construct a new ColumnLocation for the specified TableLocation and column name.
     *
     * @param tableLocation The table location enclosing this column location
     * @param name          The name of the column
     */
    ParquetColumnLocation(@NotNull final ParquetTableLocation tableLocation,
                          @NotNull final String name,
                          final ColumnChunkPageStore.Creator<ATTR> pageStoreCreator) {

        super(tableLocation, name);
        this.pageStoreCreator = pageStoreCreator;
    }

    //------------------------------------------------------------------------------------------------------------------
    // AbstractColumnLocation implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public final boolean exists() {
        if (pageStoreCreator != null) {
            synchronized (this) {
                if (pageStoreCreator != null) {
                    return pageStoreCreator.exists();
                }
            }
        }

        return pageStore != null;
    }

    @Override
    @Nullable
    public final <METADATA_TYPE> METADATA_TYPE getMetadata(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        //noinspection unchecked
        return getMetadata == null ? null : (METADATA_TYPE) getMetadata.get();
    }

    @Override
    public ColumnRegionChar<Values> makeColumnRegionChar(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionChar<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public ColumnRegionByte<Values> makeColumnRegionByte(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionByte<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public ColumnRegionShort<Values> makeColumnRegionShort(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionShort<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public ColumnRegionInt<Values> makeColumnRegionInt(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionInt<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public ColumnRegionLong<Values> makeColumnRegionLong(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionLong<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public ColumnRegionFloat<Values> makeColumnRegionFloat(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionFloat<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public ColumnRegionDouble<Values> makeColumnRegionDouble(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionDouble<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(@NotNull final ColumnDefinition<TYPE> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionObject<>((ColumnChunkPageStore<Values>) getPageStore(columnDefinition));
    }

    @Override
    public ColumnRegionInt<DictionaryKeys> makeDictionaryKeysRegion(@NotNull final ColumnDefinition<?> columnDefinition) {
        // TODO (https://github.com/deephaven/deephaven-core/issues/857): Address multiple row groups (and thus offset adjustments for multiple dictionaries)
        final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore = getDictionaryKeysPageStore(columnDefinition);
        return dictionaryKeysPageStore == null ? null : new ParquetColumnRegionInt<>(dictionaryKeysPageStore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <TYPE> ColumnRegionObject<TYPE, Values> makeDictionaryRegion(@NotNull final ColumnDefinition<?> columnDefinition) {
        // TODO (https://github.com/deephaven/deephaven-core/issues/857): Address multiple row groups (and thus multiple dictionary pages)
        final Chunk<Values> dictionaryValuesChunk = (Chunk<Values>) getDictionary(columnDefinition);
        return dictionaryValuesChunk == null ? null : ParquetColumnRegionSymbolTable.create(columnDefinition.getDataType(), dictionaryValuesChunk);
    }

    /**
     * Get the {@link ColumnChunkPageStore} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page store
     */
    @NotNull
    public final ColumnChunkPageStore<ATTR> getPageStore(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return pageStore;
    }

    /**
     * Get the {@link Dictionary} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The dictionary, or null if it doesn't exist
     */
    @Nullable
    public Chunk<ATTR> getDictionary(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionary;
    }

    /**
     * Get the {@link ColumnChunkPageStore} backing the indices for this column location. Only usable when there's
     * a dictionary.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page store
     */
    @Nullable
    private ColumnChunkPageStore<DictionaryKeys> getDictionaryKeysPageStore(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaryKeysPageStore;
    }

    private void fetchValues(@NotNull final ColumnDefinition<?> columnDefinition) {
        try {
            if (pageStoreCreator != null) {
                synchronized (this) {
                    if (pageStoreCreator != null) {
                        final ColumnChunkPageStore.Values<ATTR> values = pageStoreCreator.get(columnDefinition, ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK);
                        pageStore = values.pageStore;
                        dictionary = values.dictionary;
                        dictionaryKeysPageStore = values.dictionaryKeysPageStore;
                        getMetadata = values.getMetadata;
                        pageStoreCreator = null;
                    }
                }
            }
        } catch (IOException except) {
            throw new TableDataException("IO error reading parquet file in " + this, except);
        }
    }

    public static final class MetaDataTableFactory {

        private final ColumnChunkPageStore<Values> keyColumn;
        private final ColumnChunkPageStore<Attributes.UnorderedKeyIndices> firstColumn;
        private final ColumnChunkPageStore<Attributes.UnorderedKeyIndices> lastColumn;

        private volatile Object metaData;

        MetaDataTableFactory(@NotNull final ColumnChunkPageStore<Values> keyColumn,
                             @NotNull final ColumnChunkPageStore<Attributes.UnorderedKeyIndices> firstColumn,
                             @NotNull final ColumnChunkPageStore<Attributes.UnorderedKeyIndices> lastColumn) {
            this.keyColumn = Require.neqNull(keyColumn, "keyColumn");
            this.firstColumn = Require.neqNull(firstColumn, "firstColumn");
            this.lastColumn = Require.neqNull(lastColumn, "lastColumn");
        }

        public Object get() {
            if (metaData == null) {
                synchronized (this) {
                    if (metaData == null) {

                        final int numRows = (int) keyColumn.length();

                        try (final ChunkBoxer.BoxerKernel boxerKernel = ChunkBoxer.getBoxer(keyColumn.getChunkType(), CHUNK_SIZE);
                             final BuildGrouping buildGrouping = BuildGrouping.builder(firstColumn.getChunkType(), numRows);
                             final ChunkSource.GetContext keyContext = keyColumn.makeGetContext(CHUNK_SIZE);
                             final ChunkSource.GetContext firstContext = firstColumn.makeGetContext(CHUNK_SIZE);
                             final ChunkSource.GetContext lastContext = lastColumn.makeGetContext(CHUNK_SIZE);
                             final OrderedKeys rows = OrderedKeys.forRange(0, numRows - 1);
                             final OrderedKeys.Iterator rowsIterator = rows.getOrderedKeysIterator()) {

                            while (rowsIterator.hasMore()) {
                                final OrderedKeys chunkRows = rowsIterator.getNextOrderedKeysWithLength(CHUNK_SIZE);

                                buildGrouping.build(boxerKernel.box(keyColumn.getChunk(keyContext, chunkRows)),
                                        firstColumn.getChunk(firstContext, chunkRows),
                                        lastColumn.getChunk(lastContext, chunkRows));
                            }

                            metaData = buildGrouping.getGrouping();
                        }
                    }
                }
            }
            return metaData;
        }

        private interface BuildGrouping extends Context {
            void build(@NotNull ObjectChunk<?, ? extends Values> keyChunk,
                       @NotNull Chunk<? extends Attributes.UnorderedKeyIndices> firstChunk,
                       @NotNull Chunk<? extends Attributes.UnorderedKeyIndices> lastChunk);

            Object getGrouping();

            static BuildGrouping builder(@NotNull final ChunkType chunkType, final int numRows) {
                switch (chunkType) {
                    case Int:
                        return new IntBuildGrouping(numRows);
                    case Long:
                        return new LongBuildGrouping(numRows);
                    default:
                        throw new IllegalArgumentException("Unknown type for an index: " + chunkType);
                }
            }

            final class IntBuildGrouping implements BuildGrouping {

                private final Map<Object, int[]> grouping;

                IntBuildGrouping(final int numRows) {
                    grouping = new LinkedHashMap<>(numRows);
                }

                @Override
                public void build(@NotNull final ObjectChunk<?, ? extends Values> keyChunk,
                                  @NotNull final Chunk<? extends Attributes.UnorderedKeyIndices> firstChunk,
                                  @NotNull final Chunk<? extends Attributes.UnorderedKeyIndices> lastChunk) {
                    final IntChunk<? extends Attributes.UnorderedKeyIndices> firstIntChunk = firstChunk.asIntChunk();
                    final IntChunk<? extends Attributes.UnorderedKeyIndices> lastIntChunk = lastChunk.asIntChunk();

                    for (int ki = 0; ki < keyChunk.size(); ++ki) {
                        final int[] range = new int[2];

                        range[0] = firstIntChunk.get(ki);
                        range[1] = lastIntChunk.get(ki);

                        grouping.put(keyChunk.get(ki), range);
                    }
                }

                @Override
                public Object getGrouping() {
                    return grouping;
                }
            }

            final class LongBuildGrouping implements BuildGrouping {

                private final Map<Object, long[]> grouping;

                LongBuildGrouping(final int numRows) {
                    grouping = new LinkedHashMap<>(numRows);
                }

                @Override
                public void build(@NotNull final ObjectChunk<?, ? extends Values> keyChunk,
                                  @NotNull final Chunk<? extends Attributes.UnorderedKeyIndices> firstChunk,
                                  @NotNull final Chunk<? extends Attributes.UnorderedKeyIndices> lastChunk) {
                    final LongChunk<? extends Attributes.UnorderedKeyIndices> firstLongChunk = firstChunk.asLongChunk();
                    final LongChunk<? extends Attributes.UnorderedKeyIndices> lastLongChunk = lastChunk.asLongChunk();

                    for (int ki = 0; ki < keyChunk.size(); ++ki) {
                        final long[] range = new long[2];

                        range[0] = firstLongChunk.get(ki);
                        range[1] = lastLongChunk.get(ki);

                        grouping.put(keyChunk.get(ki), range);
                    }
                }

                @Override
                public Object getGrouping() {
                    return grouping;
                }
            }
        }
    }
}

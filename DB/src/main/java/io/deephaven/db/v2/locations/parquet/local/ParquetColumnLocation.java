/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.impl.AbstractColumnLocation;
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

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private static final int CHUNK_SIZE = Configuration.getInstance().getIntegerForClassWithDefault(ParquetColumnLocation.class, "chunkSize", 4096);

    /**
     * Factory object needed for deferred initialization of the remaining fields. Reference serves as a barrier to ensure
     * visibility of the derived fields.
     */
    private volatile ColumnChunkPageStore.Creator<ATTR>[] pageStoreCreators;

    private ColumnChunkPageStore<ATTR>[] pageStores;
    private Chunk<ATTR>[] dictionaries;
    private ColumnChunkPageStore<DictionaryKeys>[] dictionaryKeysPageStores;
    private Supplier<?> getMetadata;

    /**
     * Construct a new ColumnLocation for the specified TableLocation and column name.
     *
     * @param tableLocation The table location enclosing this column location
     * @param name          The name of the column
     */
    ParquetColumnLocation(@NotNull final ParquetTableLocation tableLocation,
                          @NotNull final String name,
                          final ColumnChunkPageStore.Creator<ATTR>[] pageStoreCreators) {

        super(tableLocation, name);
        this.pageStoreCreators = pageStoreCreators;
    }

    //------------------------------------------------------------------------------------------------------------------
    // AbstractColumnLocation implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public final boolean exists() {
        if (pageStoreCreators != null) {
            synchronized (this) {
                if (pageStoreCreators != null) {
                    return pageStoreCreators.length > 0;
                }
            }
        }

        return true;
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
        return new ParquetColumnRegionChar<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public ColumnRegionByte<Values> makeColumnRegionByte(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionByte<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public ColumnRegionShort<Values> makeColumnRegionShort(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionShort<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public ColumnRegionInt<Values> makeColumnRegionInt(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionInt<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public ColumnRegionLong<Values> makeColumnRegionLong(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionLong<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public ColumnRegionFloat<Values> makeColumnRegionFloat(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionFloat<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public ColumnRegionDouble<Values> makeColumnRegionDouble(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionDouble<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public <TYPE> ColumnRegionObject<TYPE, Values> makeColumnRegionObject(@NotNull final ColumnDefinition<TYPE> columnDefinition) {
        //noinspection unchecked
        return new ParquetColumnRegionObject<>((ColumnChunkPageStore<Values>[]) getPageStores(columnDefinition));
    }

    @Override
    public ColumnRegionInt<DictionaryKeys> makeDictionaryKeysRegion(@NotNull final ColumnDefinition<?> columnDefinition) {
        // TODO (https://github.com/deephaven/deephaven-core/issues/857): Address multiple row groups (and thus offset adjustments for multiple dictionaries)
        final ColumnChunkPageStore<DictionaryKeys>[] dictionaryKeysPageStores = getDictionaryKeysPageStores(columnDefinition);
        return dictionaryKeysPageStores == null ? null : new ParquetColumnRegionInt<>(dictionaryKeysPageStores);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <TYPE> ColumnRegionObject<TYPE, Values> makeDictionaryRegion(@NotNull final ColumnDefinition<?> columnDefinition) {
        // TODO (https://github.com/deephaven/deephaven-core/issues/857): Address multiple row groups (and thus multiple dictionary pages)
        final Chunk<Values>[] dictionaryValuesChunks = (Chunk<Values>[]) getDictionaries(columnDefinition);
        return dictionaryValuesChunks == null ? null : ParquetColumnRegionSymbolTable.create(columnDefinition.getDataType(), dictionaryValuesChunks);
    }

    /**
     * Get the {@link ColumnChunkPageStore} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page store
     */
    @NotNull
    public final ColumnChunkPageStore<ATTR>[] getPageStores(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return pageStores;
    }

    /**
     * Get the {@link Dictionary} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The dictionary, or null if it doesn't exist
     */
    @Nullable
    private Chunk<ATTR>[] getDictionaries(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaries;
    }

    /**
     * Get the {@link ColumnChunkPageStore} backing the indices for this column location. Only usable when there's
     * a dictionary.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page store
     */
    @Nullable
    private ColumnChunkPageStore<DictionaryKeys>[] getDictionaryKeysPageStores(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaryKeysPageStores;
    }

    private void fetchValues(@NotNull final ColumnDefinition<?> columnDefinition) {
        if (pageStoreCreators == null) {
            return;
        }
        synchronized (this) {
            if (pageStoreCreators == null) {
                return;
            }
            final int n = pageStoreCreators.length;
            pageStores = new ColumnChunkPageStore[n];
            dictionaries = new Chunk[n];
            dictionaryKeysPageStores = new ColumnChunkPageStore[n];
            for (int i = 0; i < n; ++i) {
                try {
                    final ColumnChunkPageStore.Values<ATTR> values = pageStoreCreators[i].get(columnDefinition, ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK);
                    pageStores[i] = values.pageStore;
                    dictionaries[i] = values.dictionary;
                    dictionaryKeysPageStores[i] = values.dictionaryKeysPageStore;
                    if (i == 0) {  // TODO MISSING: This is pretty ugly; we only need one instance of metadata, not one per row group, so we use row group 0.
                        getMetadata = values.getMetadata;
                    }
                    pageStoreCreators[i] = null;
                } catch (IOException except) {
                    throw new TableDataException("IO error reading parquet file in " + this + " row group " + i, except);
                }
            }
            pageStoreCreators = null;
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
            if (metaData != null) {
                return metaData;
            }
            synchronized (this) {
                if (metaData != null) {
                    return metaData;
                }
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

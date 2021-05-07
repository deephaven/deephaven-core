/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.local;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.AbstractColumnLocation;
import io.deephaven.db.v2.locations.ParquetFormatColumnLocation;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.ChunkBoxer;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.deephaven.db.v2.sources.regioned.RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK;

class ParquetColumnLocation<ATTR extends Attributes.Any> extends AbstractColumnLocation<ReadOnlyParquetTableLocation> implements ParquetFormatColumnLocation<ATTR, ReadOnlyParquetTableLocation> {

    private volatile ColumnChunkPageStore.Creator<ATTR> pageStoreCreator;

    private ColumnChunkPageStore<ATTR> pageStore;
    private Chunk<ATTR> dictionary;
    private ColumnChunkPageStore<Attributes.DictionaryKeys> dictionaryKeysPageStore;
    private Supplier<?> getMetadata;

    /**
     * Construct a new ColumnLocation for the specified TableLocation and column name.
     *
     * @param tableLocation The table location enclosing this column location
     * @param name The name of the column
     */
    ParquetColumnLocation(@NotNull final ReadOnlyParquetTableLocation tableLocation,
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
    public final <METADATA_TYPE> METADATA_TYPE getMetadata(ColumnDefinition columnDefinition) {
        fetchValues(columnDefinition);
        //noinspection unchecked
        return getMetadata == null ? null : (METADATA_TYPE) getMetadata.get();
    }

    @NotNull
    @Override
    public final ColumnChunkPageStore<ATTR> getPageStore(ColumnDefinition columnDefinition) {
        fetchValues(columnDefinition);
        return pageStore;
    }

    @Override
    public final Chunk<ATTR> getDictionary(ColumnDefinition columnDefinition) {
        fetchValues(columnDefinition);
        return dictionary;
    }

    @Override
    public final ColumnChunkPageStore<Attributes.DictionaryKeys> getDictionaryKeysPageStore(ColumnDefinition columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaryKeysPageStore;
    }

    private void fetchValues(ColumnDefinition columnDefinition) {
        try {
            if (pageStoreCreator != null) {
                synchronized (this) {
                    if (pageStoreCreator != null) {
                        ColumnChunkPageStore.Values<ATTR> values = pageStoreCreator.get(columnDefinition, ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK);
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

    private static int CHUNK_SIZE = Configuration.getInstance().getIntegerForClassWithDefault(ParquetColumnLocation.class,
            "chunkSize", 4096);

    static class MetaDataTableFactory {
        private final ColumnChunkPageStore<Attributes.Values> keyColumn;
        private final ColumnChunkPageStore<Attributes.UnorderedKeyIndices> firstColumn;
        private final ColumnChunkPageStore<Attributes.UnorderedKeyIndices> lastColumn;

        private volatile Object metaData;

        MetaDataTableFactory(ColumnChunkPageStore<Attributes.Values> keyColumn,
                             ColumnChunkPageStore<Attributes.UnorderedKeyIndices> firstColumn,
                             ColumnChunkPageStore<Attributes.UnorderedKeyIndices> lastColumn) {
            this.keyColumn = Require.neqNull(keyColumn, "keyColumn");
            this.firstColumn = Require.neqNull(firstColumn, "firstColumn");
            this.lastColumn = Require.neqNull(lastColumn, "lastColumn");
        }

        public final Object get() {
            if (metaData == null) {
                synchronized (this) {
                    if (metaData == null) {

                        int numRows = (int) keyColumn.length();

                        try (ChunkBoxer.BoxerKernel boxerKernel = ChunkBoxer.getBoxer(keyColumn.getChunkType(), CHUNK_SIZE);
                             BuildGrouping buildGrouping = BuildGrouping.builder(firstColumn.getChunkType(), numRows);
                             ChunkSource.GetContext keyContext = keyColumn.makeGetContext(CHUNK_SIZE);
                             ChunkSource.GetContext firstContext = firstColumn.makeGetContext(CHUNK_SIZE);
                             ChunkSource.GetContext lastContext = lastColumn.makeGetContext(CHUNK_SIZE)) {

                            Index index = Index.FACTORY.getIndexByRange(0, numRows - 1);

                            for (OrderedKeys.Iterator iterator = index.getOrderedKeysIterator(); iterator.hasMore(); ) {
                                OrderedKeys chunkOrderedKeys = iterator.getNextOrderedKeysWithLength(CHUNK_SIZE);

                                buildGrouping.build(boxerKernel.box(keyColumn.getChunk(keyContext, chunkOrderedKeys)),
                                        firstColumn.getChunk(firstContext, chunkOrderedKeys),
                                        lastColumn.getChunk(lastContext, chunkOrderedKeys));
                            }

                            metaData = buildGrouping.getGrouping();

                        }
                    }
                }
            }

            return metaData;
        }

        private interface BuildGrouping extends Context {
            void build(ObjectChunk<?, ? extends Attributes.Values> keyChunk,
                       Chunk<? extends Attributes.UnorderedKeyIndices> firstChunk,
                       Chunk<? extends Attributes.UnorderedKeyIndices> lastChunk);

            Object getGrouping();

            static BuildGrouping builder(ChunkType chunkType, int numRows) {
                switch (chunkType) {
                    case Int:
                        return new IntBuildGrouping(numRows);

                    case Long:
                        return new LongBuildGrouping(numRows);

                    default:
                        throw new IllegalArgumentException("Unknown type for an index: " + chunkType);
                }
            }

            class IntBuildGrouping implements BuildGrouping {
                Map<Object, int []> grouping;

                IntBuildGrouping(int numRows) {
                    grouping = new LinkedHashMap<>(numRows);
                }

                @Override
                public void build(ObjectChunk<?, ? extends Attributes.Values> keyChunk,
                                  Chunk<? extends Attributes.UnorderedKeyIndices> firstChunk,
                                  Chunk<? extends Attributes.UnorderedKeyIndices> lastChunk) {
                    IntChunk<? extends Attributes.UnorderedKeyIndices> firstIntChunk = firstChunk.asIntChunk();
                    IntChunk<? extends Attributes.UnorderedKeyIndices> lastIntChunk = lastChunk.asIntChunk();

                    for (int i = 0; i < keyChunk.size(); ++i) {
                        int[] range = new int[2];

                        range[0] = firstIntChunk.get(i);
                        range[1] = lastIntChunk.get(i);

                        grouping.put(keyChunk.get(i), range);
                    }
                }

                @Override
                public Object getGrouping() {
                    return grouping;
                }
            }

            class LongBuildGrouping implements BuildGrouping {
                Map<Object, long []> grouping;

                LongBuildGrouping(int numRows) {
                    grouping = new LinkedHashMap<>(numRows);
                }

                @Override
                public void build(ObjectChunk<?, ? extends Attributes.Values> keyChunk,
                                  Chunk<? extends Attributes.UnorderedKeyIndices> firstChunk,
                                  Chunk<? extends Attributes.UnorderedKeyIndices> lastChunk) {
                    LongChunk<? extends Attributes.UnorderedKeyIndices> firstLongChunk = firstChunk.asLongChunk();
                    LongChunk<? extends Attributes.UnorderedKeyIndices> lastLongChunk = lastChunk.asLongChunk();

                    for (int i = 0; i < keyChunk.size(); ++i) {
                        long[] range = new long[2];

                        range[0] = firstLongChunk.get(i);
                        range[1] = lastLongChunk.get(i);

                        grouping.put(keyChunk.get(i), range);
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

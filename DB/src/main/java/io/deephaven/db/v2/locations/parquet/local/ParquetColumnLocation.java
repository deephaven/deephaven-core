/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.CodecLookup;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.impl.AbstractColumnLocation;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.locations.parquet.topage.*;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import io.deephaven.db.v2.sources.chunk.Attributes.UnorderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.regioned.*;
import io.deephaven.db.v2.utils.ChunkBoxer;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.parquet.ColumnChunkReader;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.RowGroupReader;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;

final class ParquetColumnLocation<ATTR extends Any> extends AbstractColumnLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private static final int CHUNK_SIZE = Configuration.getInstance().getIntegerForClassWithDefault(ParquetColumnLocation.class, "chunkSize", 4096);

    private final String parquetColumnName;
    /**
     * Factory object needed for deferred initialization of the remaining fields. Reference serves as a barrier to ensure
     * visibility of the derived fields.
     */
    private volatile ColumnChunkReader[] columnChunkReaders;
    private final boolean hasGroupingTable;

    private ColumnChunkPageStore<ATTR>[] pageStores;
    private Chunk<ATTR>[] dictionaries;
    private ColumnChunkPageStore<DictionaryKeys>[] dictionaryKeysPageStores;
    private RegionedPageStore.Parameters regionParameters;

    /**
     * Construct a new {@link ParquetColumnLocation} for the specified {@link ParquetTableLocation} and column name.
     *
     * @param tableLocation      The table location enclosing this column location
     * @param parquetColumnName  The Parquet file column name
     * @param columnChunkReaders The {@link ColumnChunkReader column chunk readers} for this location
     * @param hasGroupingTable   Whether this column has an associated grouping table file
     */
    ParquetColumnLocation(@NotNull final ParquetTableLocation tableLocation,
                          @NotNull final String columnName,
                          @NotNull final String parquetColumnName,
                          @Nullable final ColumnChunkReader[] columnChunkReaders,
                          final boolean hasGroupingTable) {
        super(tableLocation, columnName);
        this.parquetColumnName = parquetColumnName;
        this.columnChunkReaders = columnChunkReaders;
        this.hasGroupingTable = hasGroupingTable;
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
        // If we see a null columnChunkReaders array, either we don't exist or we are guaranteed to see a non-null
        // pageStores array
        return columnChunkReaders != null || pageStores != null;
    }

    private static final ColumnDefinition<Long> FIRST_KEY_COL_DEF = ColumnDefinition.ofLong("__firstKey__");
    private static final ColumnDefinition<Long> LAST_KEY_COL_DEF = ColumnDefinition.ofLong("__lastKey__");

    @Override
    @Nullable
    public final <METADATA_TYPE> METADATA_TYPE getMetadata(@NotNull final ColumnDefinition<?> columnDefinition) {
        if (!hasGroupingTable) {
            return null;
        }

        final ParquetTableLocation parquetTableLocation = (ParquetTableLocation) getTableLocation();
        final Function<String, String> defaultGroupingFilenameByColumnName =
                ParquetTableWriter.defaultGroupingFileName(parquetTableLocation.getParquetFile().getAbsolutePath());
        try {
            final ParquetFileReader parquetFileReader = new ParquetFileReader(
                    defaultGroupingFilenameByColumnName.apply(parquetColumnName), parquetTableLocation.getChannelProvider(), -1);
            final Map<String, String> keyValueMetaData = new ParquetMetadataConverter().fromParquetMetadata(parquetFileReader.fileMetaData).getFileMetaData().getKeyValueMetaData();

            final RowGroupReader rowGroupReader = parquetFileReader.getRowGroup(0);
            final ColumnChunkReader groupingKeyReader = rowGroupReader.getColumnChunk(Collections.singletonList(ParquetTableWriter.GROUPING_KEY));
            final ColumnChunkReader beginPosReader = rowGroupReader.getColumnChunk(Collections.singletonList(ParquetTableWriter.BEGIN_POS));
            final ColumnChunkReader endPosReader = rowGroupReader.getColumnChunk(Collections.singletonList(ParquetTableWriter.END_POS));
            if (groupingKeyReader == null || beginPosReader == null || endPosReader == null) {
                // "hasGroupingTable" is wrong; bail out here rather than blowing up
                return null;
            }

            //noinspection unchecked
            return (METADATA_TYPE) new MetaDataTableFactory(
                    ColumnChunkPageStore.<Values>create(groupingKeyReader, RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK,
                            makeToPage(keyValueMetaData, ParquetInstructions.EMPTY, ParquetTableWriter.GROUPING_KEY, groupingKeyReader, columnDefinition)).pageStore,
                    ColumnChunkPageStore.<UnorderedKeyIndices>create(beginPosReader, RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK,
                            makeToPage(keyValueMetaData, ParquetInstructions.EMPTY, ParquetTableWriter.BEGIN_POS, beginPosReader, FIRST_KEY_COL_DEF)).pageStore,
                    ColumnChunkPageStore.<UnorderedKeyIndices>create(endPosReader, RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK,
                            makeToPage(keyValueMetaData, ParquetInstructions.EMPTY, ParquetTableWriter.END_POS, beginPosReader, LAST_KEY_COL_DEF)).pageStore
            ).get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ColumnRegionChar<Values> makeColumnRegionChar(@NotNull final ColumnDefinition<?> columnDefinition) {
        //noinspection unchecked
        return new ColumnRegionChar.StaticPageStore<>(regionParameters, Arrays.stream(getPageStores(columnDefinition)).map(
                ccps -> ccps == null ? ColumnRegionChar.<Values>createNull(regionParameters.regionMask) : new ParquetColumnRegionChar<>(ccps)
        ).toArray(ColumnRegionChar[]::new));
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
        // TODO-RWC: This is insufficient. We need to add the length of prior dictionaries to keys.
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
     * Get the {@link ColumnChunkPageStore page stores} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page stores
     */
    @NotNull
    public final ColumnChunkPageStore<ATTR>[] getPageStores(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return pageStores;
    }

    /**
     * Get the {@link Dictionary dictionaries} backing this column location.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The dictionaries, or null if none exist
     */
    @Nullable
    public Chunk<ATTR>[] getDictionaries(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaries;
    }

    /**
     * Get the {@link ColumnChunkPageStore page stores} backing the indices for this column location.
     * Only usable when there are dictionaries.
     *
     * @param columnDefinition The {@link ColumnDefinition} used to lookup type information
     * @return The page stores
     */
    @Nullable
    private ColumnChunkPageStore<DictionaryKeys>[] getDictionaryKeysPageStores(@NotNull final ColumnDefinition<?> columnDefinition) {
        fetchValues(columnDefinition);
        return dictionaryKeysPageStores;
    }

    @SuppressWarnings("unchecked")
    private void fetchValues(@NotNull final ColumnDefinition<?> columnDefinition) {
        if (columnChunkReaders == null) {
            return;
        }
        synchronized (this) {
            if (columnChunkReaders == null) {
                return;
            }

            final int pageStoreCount = columnChunkReaders.length;
            regionParameters = new RegionedPageStore.Parameters(
                    RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK,
                    pageStoreCount,
                    Arrays.stream(columnChunkReaders).filter(Objects::nonNull).mapToLong(ColumnChunkReader::numRows).max().orElseThrow(IllegalStateException::new));
            final ParquetTableLocation parquetTableLocation = (ParquetTableLocation) getTableLocation();

            pageStores = new ColumnChunkPageStore[pageStoreCount];
            dictionaries = new Chunk[pageStoreCount];
            dictionaryKeysPageStores = new ColumnChunkPageStore[pageStoreCount];
            for (int psi = 0; psi < pageStoreCount; ++psi) {
                final ColumnChunkReader columnChunkReader = columnChunkReaders[psi];
                try {
                    final ColumnChunkPageStore.CreatorResult<ATTR> creatorResult = ColumnChunkPageStore.create(
                            columnChunkReader,
                            regionParameters.regionMask,
                            makeToPage(parquetTableLocation.getKeyValueMetaData(), parquetTableLocation.getReadInstructions(), parquetColumnName, columnChunkReader, columnDefinition));
                    pageStores[psi] = creatorResult.pageStore;
                    dictionaries[psi] = creatorResult.dictionary;
                    dictionaryKeysPageStores[psi] = creatorResult.dictionaryKeysPageStore;
                } catch (IOException e) {
                    throw new TableDataException("Failed to read parquet file for " + this + ", row group " + psi, e);
                }
            }

            columnChunkReaders = null;
        }
    }

    private static final class MetaDataTableFactory {

        private final ColumnChunkPageStore<Values> keyColumn;
        private final ColumnChunkPageStore<UnorderedKeyIndices> firstColumn;
        private final ColumnChunkPageStore<UnorderedKeyIndices> lastColumn;

        private volatile Object metaData;

        private MetaDataTableFactory(@NotNull final ColumnChunkPageStore<Values> keyColumn,
                                     @NotNull final ColumnChunkPageStore<UnorderedKeyIndices> firstColumn,
                                     @NotNull final ColumnChunkPageStore<UnorderedKeyIndices> lastColumn) {
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
                final int numRows = (int) keyColumn.size();

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
                       @NotNull Chunk<? extends UnorderedKeyIndices> firstChunk,
                       @NotNull Chunk<? extends UnorderedKeyIndices> lastChunk);

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
                                  @NotNull final Chunk<? extends UnorderedKeyIndices> firstChunk,
                                  @NotNull final Chunk<? extends UnorderedKeyIndices> lastChunk) {
                    final IntChunk<? extends UnorderedKeyIndices> firstIntChunk = firstChunk.asIntChunk();
                    final IntChunk<? extends UnorderedKeyIndices> lastIntChunk = lastChunk.asIntChunk();

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
                                  @NotNull final Chunk<? extends UnorderedKeyIndices> firstChunk,
                                  @NotNull final Chunk<? extends UnorderedKeyIndices> lastChunk) {
                    final LongChunk<? extends UnorderedKeyIndices> firstLongChunk = firstChunk.asLongChunk();
                    final LongChunk<? extends UnorderedKeyIndices> lastLongChunk = lastChunk.asLongChunk();

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

    private static <ATTR extends Any, RESULT> ToPage<ATTR, RESULT> makeToPage(@NotNull final Map<String, String> keyValueMetaData,
                                                                              @NotNull final ParquetInstructions readInstructions,
                                                                              @NotNull final String parquetColumnName,
                                                                              @NotNull final ColumnChunkReader columnChunkReader,
                                                                              @NotNull final ColumnDefinition columnDefinition) {
        final PrimitiveType type = columnChunkReader.getType();
        final LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
        final String codecFromInstructions = readInstructions.getCodecName(columnDefinition.getName());
        final String codecName = (codecFromInstructions != null)
                ? codecFromInstructions
                : keyValueMetaData.get(ParquetTableWriter.CODEC_NAME_PREFIX + parquetColumnName);
        final String specialTypeName = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + parquetColumnName);

        final boolean isArray = columnChunkReader.getMaxRl() > 0;
        final boolean isCodec = CodecLookup.explicitCodecPresent(codecName);

        if (isArray && columnChunkReader.getMaxRl() > 1) {
            throw new TableDataException("No support for nested repeated parquet columns.");
        }

        try {
            // Note that componentType is null for a StringSet. ToStringSetPage.create specifically doesn't take this parameter.
            final Class<?> dataType = columnDefinition.getDataType();
            final Class<?> componentType = columnDefinition.getComponentType();
            final Class<?> pageType = isArray ? componentType : dataType;

            ToPage<ATTR, ?> toPage = null;

            if (logicalTypeAnnotation != null) {
                toPage = logicalTypeAnnotation.accept(new LogicalTypeVisitor<ATTR>(parquetColumnName, columnChunkReader, pageType)).orElse(null);
            }

            if (toPage == null) {
                final PrimitiveType.PrimitiveTypeName typeName = type.getPrimitiveTypeName();
                switch (typeName) {
                    case BOOLEAN:
                        toPage = ToBooleanAsBytePage.create(pageType);
                        break;
                    case INT32:
                        toPage = ToIntPage.create(pageType);
                        break;
                    case INT64:
                        toPage = ToLongPage.create(pageType);
                        break;
                    case INT96:
                        toPage = ToDBDateTimePageFromInt96.create(pageType);
                        break;
                    case DOUBLE:
                        toPage = ToDoublePage.create(pageType);
                        break;
                    case FLOAT:
                        toPage = ToFloatPage.create(pageType);
                        break;
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        //noinspection rawtypes
                        final ObjectCodec codec;
                        if (isCodec) {
                            final String codecArgs = (codecFromInstructions != null)
                                    ? readInstructions.getCodecArgs(columnDefinition.getName())
                                    : keyValueMetaData.get(ParquetTableWriter.CODEC_ARGS_PREFIX + parquetColumnName);
                            codec = CodecCache.DEFAULT.getCodec(codecName, codecArgs);
                        } else {
                            final String codecArgs = (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                    ? Integer.toString(type.getTypeLength())
                                    : null;
                            codec = CodecCache.DEFAULT.getCodec(SimpleByteArrayCodec.class.getName(), codecArgs);
                        }
                        //noinspection unchecked
                        toPage = ToObjectPage.create(dataType, codec, columnChunkReader.getDictionary());
                        break;
                    default:
                }
            }

            if (toPage == null) {
                throw new TableDataException("Unsupported parquet column type " + type.getPrimitiveTypeName() +
                        " with logical type " + logicalTypeAnnotation);
            }

            if (Objects.equals(specialTypeName, ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                Assert.assertion(isArray, "isArray");
                toPage = ToStringSetPage.create(dataType, toPage);
            } else if (isArray) {
                Assert.assertion(!isCodec, "!isCodec");
                if (DbArrayBase.class.isAssignableFrom(dataType)) {
                    toPage = ToDbArrayPage.create(dataType, componentType, toPage);
                } else if (dataType.isArray()) {
                    toPage = ToArrayPage.create(dataType, componentType, toPage);
                }
            }

            //noinspection unchecked
            return (ToPage<ATTR, RESULT>) toPage;

        } catch (IOException except) {
            throw new TableDataException("IO exception accessing column " + parquetColumnName, except);
        } catch (RuntimeException except) {
            throw new TableDataException("Unexpected exception accessing column " + parquetColumnName, except);
        }
    }

    private static class LogicalTypeVisitor<ATTR extends Any> implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ToPage<ATTR, ?>> {

        private final String name;
        private final ColumnChunkReader columnChunkReader;
        private final Class<?> componentType;

        LogicalTypeVisitor(@NotNull String name, @NotNull ColumnChunkReader columnChunkReader, Class<?> componentType) {
            this.name = name;
            this.columnChunkReader = columnChunkReader;
            this.componentType = componentType;
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            try {
                return Optional.of(ToStringPage.create(componentType, columnChunkReader.getDictionary()));
            } catch (IOException except) {
                throw new TableDataException("Failure accessing string column " + name, except);
            }
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            if (timestampLogicalType.isAdjustedToUTC()) {
                return Optional.of(ToDBDateTimePage.create(componentType, timestampLogicalType.getUnit()));
            }

            throw new TableDataException("Timestamp column is not UTC or is not nanoseconds " + name);
        }

        @Override
        public Optional<ToPage<ATTR, ?>> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {

            if (intLogicalType.isSigned()) {
                switch (intLogicalType.getBitWidth()) {
                    case 8:
                        return Optional.of(ToBytePageFromInt.create(componentType));
                    case 16:
                        return Optional.of(ToShortPageFromInt.create(componentType));
                    case 32:
                        return Optional.of(ToIntPage.create(componentType));
                    case 64:
                        return Optional.of(ToLongPage.create(componentType));
                }
            } else {
                switch (intLogicalType.getBitWidth()) {
                    case 8:
                    case 16:
                        return Optional.of(ToCharPageFromInt.create(componentType));
                    case 32:
                        return Optional.of(ToLongPage.create(componentType));
                }
            }

            return Optional.empty();
        }
    }
}

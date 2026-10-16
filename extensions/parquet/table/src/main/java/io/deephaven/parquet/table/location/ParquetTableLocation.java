//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.LongChunkMatchFilterFactory;
import io.deephaven.engine.table.impl.dataindex.StandaloneDataIndex;
import io.deephaven.engine.table.impl.filter.ExtractFilterWithoutBarriers;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.parquet.base.RowGroupReader;
import io.deephaven.parquet.impl.ParquetSchemaUtil;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetSchemaReader;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.metadata.DataIndexInfo;
import io.deephaven.parquet.table.metadata.GroupingColumnInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableLong;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;
import static io.deephaven.parquet.table.ParquetTableWriter.*;
import static io.deephaven.parquet.table.ParquetTableWriter.GROUPING_END_POS_COLUMN_NAME;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class ParquetTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private static final Logger log = LoggerFactory.getLogger(ParquetTableLocation.class);

    private final ParquetInstructions readInstructions;

    private volatile boolean isInitialized;

    // Access to all the following variables must be guarded by initialize()
    // -----------------------------------------------------------------------
    /** Finds each Deephaven column in this file, and translates the file's metadata to Deephaven column names. */
    private ParquetTableLocationColumnResolver resolver;

    private RegionedPageStore.Parameters regionParameters;

    /**
     * The Deephaven metadata in the file footer. It names columns by their parquet names -- the names in the file's
     * schema -- which differ from the Deephaven names whenever the reader renames or resolves columns; translate with
     * the {@link #resolver} before crossing that boundary.
     */
    private TableInfo tableInfo;
    /** Keyed by parquet column name. */
    private Map<String, GroupingColumnInfo> groupingColumns;
    /** Keyed by parquet column name. */
    private Map<String, ColumnTypeInfo> columnTypes;

    private ParquetFileReader parquetFileReader;
    private ParquetMetadata parquetMetadata;
    /**
     * The indices, in the file's metadata, of this location's row groups, in the order of this location's regions:
     * entry {@code i} is the row group backing region {@code i}. These index the file-wide row group and block lists,
     * which for a {@code _metadata} layout cover every file in the dataset.
     */
    private int[] rowGroupIndices;
    private MessageType parquetSchema;
    // -----------------------------------------------------------------------

    private volatile RowGroupReader[] rowGroupReaders;

    /**
     * The file's sort columns, translated to Deephaven column names on first use; see {@link #getSortedColumns()}.
     */
    private volatile List<SortColumn> sortingColumns;

    public ParquetTableLocation(@NotNull final TableKey tableKey,
            @NotNull final ParquetTableLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, false);
        this.readInstructions = readInstructions;
        this.isInitialized = false;
    }

    private void initialize() {
        if (isInitialized) {
            return;
        }
        synchronized (this) {
            if (isInitialized) {
                return;
            }
            final ParquetTableLocationKey tableLocationKey = getParquetKey();
            synchronized (tableLocationKey) {
                // Following methods are internally synchronized, we synchronize them together here to minimize
                // lock/unlock calls
                parquetFileReader = tableLocationKey.getFileReader();
                parquetMetadata = tableLocationKey.getMetadata();
                // Region i reads row group rowGroupIndices[i], and every per-region structure -- region sizes,
                // row group readers, statistics -- is built from this one array, so they agree. Regions follow the
                // row groups' ordinals, their positions in the file, as they always have. A writer that leaves the
                // ordinal unset leaves it 0 throughout, and the stable sort keeps the indices in their order.
                final List<RowGroup> fileRowGroups = parquetFileReader.fileMetaData.getRow_groups();
                rowGroupIndices = IntStream.of(tableLocationKey.getRowGroupIndices())
                        .boxed()
                        .sorted(Comparator.comparingInt(rgi -> fileRowGroups.get(rgi).getOrdinal()))
                        .mapToInt(Integer::intValue)
                        .toArray();
            }

            final int rowGroupCount = rowGroupIndices.length;
            final RowGroup[] rowGroups = IntStream.of(rowGroupIndices)
                    .mapToObj(rgi -> parquetFileReader.fileMetaData.getRow_groups().get(rgi))
                    .toArray(RowGroup[]::new);
            final long maxRowCount = Arrays.stream(rowGroups).mapToLong(RowGroup::getNum_rows).max().orElse(0L);
            regionParameters = new RegionedPageStore.Parameters(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, rowGroupCount, maxRowCount);

            parquetSchema = parquetFileReader.getSchema();

            final Map<String, String[]> pathsByParquetColumnName = new HashMap<>();
            for (String[] path : ParquetSchemaUtil.paths(parquetSchema)) {
                if (path.length > 1) {
                    pathsByParquetColumnName.put(path[0], path);
                }
            }
            resolver = new ParquetTableLocationColumnResolver(
                    readInstructions,
                    parquetSchema,
                    pathsByParquetColumnName,
                    readInstructions.getColumnResolverFactory()
                            .map(factory -> factory.of(getTableKey(), tableLocationKey))
                            .orElse(null),
                    tableLocationKey.getPartitionKeys());

            tableInfo = ParquetSchemaReader
                    .parseMetadata(parquetMetadata.getFileMetaData().getKeyValueMetaData())
                    .orElse(TableInfo.builder().build());
            groupingColumns = tableInfo.groupingColumnMap();
            columnTypes = tableInfo.columnTypeMap();

            if (!FILE_URI_SCHEME.equals(tableLocationKey.getURI().getScheme())) {
                // We do not have the last modified time for non-file URIs
                handleUpdateInternal(computeRowSet(rowGroups), TableLocationState.NULL_TIME);
            } else {
                handleUpdateInternal(computeRowSet(rowGroups), new File(tableLocationKey.getURI()).lastModified());
            }

            isInitialized = true;
        }
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {}

    ParquetTableLocationKey getParquetKey() {
        return (ParquetTableLocationKey) getKey();
    }

    ParquetInstructions getReadInstructions() {
        return readInstructions;
    }

    RegionedPageStore.Parameters getRegionParameters() {
        initialize();
        return regionParameters;
    }

    /**
     * Returns the file's column type information, keyed by parquet column name: look a column up by the parquet column
     * it reads, which may be read by more than one Deephaven column.
     */
    Map<String, ColumnTypeInfo> getColumnTypes() {
        initialize();
        return columnTypes;
    }

    RowGroupReader[] getRowGroupReaders() {
        RowGroupReader[] local;
        if ((local = rowGroupReaders) != null) {
            return local;
        }
        synchronized (this) {
            if ((local = rowGroupReaders) != null) {
                return local;
            }
            initialize();
            local = IntStream.of(rowGroupIndices)
                    .mapToObj(idx -> parquetFileReader.getRowGroup(idx, tableInfo.version()))
                    .toArray(RowGroupReader[]::new);

            // We don't need this anymore
            parquetFileReader = null;

            rowGroupReaders = local;
            return local;
        }
    }

    @Override
    @NotNull
    public List<SortColumn> getSortedColumns() {
        List<SortColumn> local;
        if ((local = sortingColumns) != null) {
            return local;
        }
        initialize();
        synchronized (this) {
            if ((local = sortingColumns) != null) {
                return local;
            }
            return sortingColumns = resolver.translateToDeephavenSortColumns(tableInfo.sortingColumns());
        }
    }

    @Override
    protected final void initializeState() {
        initialize();
    }

    @Override
    @NotNull
    protected ColumnLocation makeColumnLocation(@NotNull final String columnName) {
        return new ParquetColumnLocation<>(this, columnName);
    }

    /**
     * Returns the path to the leaf of this file's parquet schema that the Deephaven column {@code columnName} reads, or
     * an empty list if it reads none.
     */
    List<String> getColumnPath(@NotNull final String columnName) {
        initialize();
        // In the future, we could make this more granular so that the resolver can be constructed without calling
        // initialize first.
        // empty list will result in exists=false
        return resolver.of(columnName).orElse(List.of());
    }

    private RowSet computeRowSet(@NotNull final RowGroup[] rowGroups) {
        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();

        for (int rgi = 0; rgi < rowGroups.length; ++rgi) {
            final long subRegionSize = rowGroups[rgi].getNum_rows();
            if (subRegionSize == 0) {
                // Skip empty row groups
                continue;
            }
            final long subRegionFirstKey = getSubRegionFirstKey(rgi);
            final long subRegionLastKey = subRegionFirstKey + subRegionSize - 1;
            sequentialBuilder.appendRange(subRegionFirstKey, subRegionLastKey);
        }
        return sequentialBuilder.build();
    }

    private long getSubRegionFirstKey(final long rowGroupIdx) {
        return rowGroupIdx << regionParameters.regionMaskNumBits;
    }

    @Override
    @NotNull
    public List<String[]> getDataIndexColumns() {
        initialize();
        if (tableInfo.dataIndexes().isEmpty() && groupingColumns.isEmpty()) {
            return List.of();
        }
        final List<String[]> dataIndexColumns =
                new ArrayList<>(tableInfo.dataIndexes().size() + groupingColumns.size());
        // The metadata names parquet columns, and callers expect Deephaven column names. An index whose columns cannot
        // all be identified is omitted.
        // Add the data indexes to the list
        tableInfo.dataIndexes().stream()
                .map(di -> resolver.findIndexDeephavenColumnNames(di.columns()))
                .filter(Objects::nonNull)
                .forEach(dataIndexColumns::add);
        // Add grouping columns to the list
        groupingColumns.keySet().stream()
                .map(parquetColumnName -> resolver.findIndexDeephavenColumnNames(List.of(parquetColumnName)))
                .filter(Objects::nonNull)
                .forEach(dataIndexColumns::add);
        return dataIndexColumns;
    }

    /**
     * {@inheritDoc}
     * <p>
     * An index is trusted when this location's own file declares it and the index file exists; nothing else about the
     * index is checked against the file it indexes, and {@link #pushdownDataIndex} treats every row the index does not
     * place under a matching key as not matching. Every index a file declares must therefore have been written for that
     * file: a writer that replaces the file, or an index it declares, without the other makes filters silently drop
     * rows. An index file that no current file declares, such as one left behind when a file is rewritten without
     * indexes, is ignored. Deephaven's own writers commit a file and the indexes it declares together.
     */
    @Override
    public boolean hasDataIndex(@NotNull final String... columns) {
        initialize();
        if (tableInfo.dataIndexes().isEmpty() && groupingColumns.isEmpty()) {
            return false;
        }
        // The metadata names parquet columns
        final String[] parquetColumnNames = resolver.findIndexParquetColumnNames(Arrays.asList(columns));
        if (parquetColumnNames == null) {
            return false;
        }
        // Check if the column name matches any of the grouping columns
        if (parquetColumnNames.length == 1 && groupingColumns.containsKey(parquetColumnNames[0])) {
            // Validate the index file exists (without loading and parsing it)
            final IndexFileMetadata metadata = getIndexFileMetadata(getParquetKey().getURI(), parquetColumnNames);
            return metadata != null && parquetFileExists(metadata.fileURI);
        }
        // Check if the column names match any of the data indexes
        for (final DataIndexInfo dataIndex : tableInfo.dataIndexes()) {
            if (dataIndex.matchesColumns(parquetColumnNames)) {
                // Validate the index file exists (without loading and parsing it)
                final IndexFileMetadata metadata = getIndexFileMetadata(getParquetKey().getURI(), parquetColumnNames);
                return metadata != null && parquetFileExists(metadata.fileURI);
            }
        }
        return false;
    }

    private static boolean parquetFileExists(@NotNull final URI fileURI) {
        // We assume non-file URIs are always present
        return !fileURI.getScheme().equals(FILE_URI_SCHEME) || Files.exists(Path.of(fileURI));
    }

    @Override
    @Nullable
    public BasicDataIndex loadDataIndex(@NotNull final String... columns) {
        initialize();
        // The metadata names parquet columns
        final String[] parquetColumnNames = resolver.findIndexParquetColumnNames(Arrays.asList(columns));
        final IndexFileMetadata indexFileMetaData = parquetColumnNames == null
                ? null
                : getIndexFileMetadata(getParquetKey().getURI(), parquetColumnNames);
        if (indexFileMetaData == null) {
            throw new TableDataException(
                    String.format(
                            "No index metadata for table %s with index key columns %s was present in TableInfo",
                            getParquetKey().getURI(), Arrays.toString(columns)));
        }
        // Create a new index from the parquet table
        final Table table =
                readDataIndexTable(getParquetKey().getURI(), indexFileMetaData, columns);
        if (table == null) {
            return null;
        }
        final RowSet locationRowSet = getRowSet();
        if (locationRowSet.isFlat()) {
            // Index row positions are row keys, so the index table needs no adjustment.
            locationRowSet.close();
            return StandaloneDataIndex.from(table, columns, INDEX_ROW_SET_COLUMN_NAME);
        }
        // The adjusted table maps positions to row keys lazily, so it keeps the location's row set for its lifetime.
        // If the index cannot be built, nothing owns the row set, so close it here.
        try {
            final Table adjustedTable = table.updateView(List.of(new FunctionalColumn<>(
                    INDEX_ROW_SET_COLUMN_NAME, RowSet.class,
                    INDEX_ROW_SET_COLUMN_NAME, RowSet.class,
                    (final RowSet indexRowSet) -> locationRowSet.subSetForPositions(indexRowSet))));
            return StandaloneDataIndex.from(adjustedTable, columns, INDEX_ROW_SET_COLUMN_NAME);
        } catch (final RuntimeException | Error e) {
            SafeCloseable.closeAllDuringFailure(e, locationRowSet);
            throw e;
        }
    }

    private static class IndexFileMetadata {

        private final URI fileURI;
        private final GroupingColumnInfo groupingColumnInfo;
        private final DataIndexInfo dataIndexInfo;

        private IndexFileMetadata(
                @NotNull final URI fileURI,
                @Nullable final GroupingColumnInfo groupingColumnInfo,
                @Nullable final DataIndexInfo dataIndexInfo) {
            this.fileURI = fileURI;
            Require.requirement(groupingColumnInfo != null ^ dataIndexInfo != null,
                    "Exactly one of groupingColumnInfo and dataIndexInfo must be non-null");
            this.groupingColumnInfo = groupingColumnInfo;
            this.dataIndexInfo = dataIndexInfo;
        }
    }

    private static URI makeRelativeURI(@NotNull final URI parentFileURI, @NotNull final String relativePath) {
        final String parentPath = parentFileURI.getPath();
        final int lastSlashIndex = parentPath.lastIndexOf('/');
        try {
            return new URI(parentFileURI.getScheme(), parentFileURI.getAuthority(),
                    (lastSlashIndex == -1 ? parentPath : parentPath.substring(0, lastSlashIndex + 1)) + relativePath,
                    null, null);
        } catch (URISyntaxException e) {
            throw new TableDataException(String.format("Failed to format relative URI for parent %s, child %s",
                    parentFileURI, relativePath), e);
        }
    }

    /**
     * Returns the metadata of the index file the footer declares for the given key columns, or {@code null} if it
     * declares none.
     *
     * @param parentFileURI The path to the base table
     * @param parquetKeyColumnNames The parquet names of the key columns, as the footer records them
     */
    @Nullable
    private IndexFileMetadata getIndexFileMetadata(
            @NotNull final URI parentFileURI,
            @NotNull final String... parquetKeyColumnNames) {
        if (parquetKeyColumnNames.length == 1) {
            // If there's only one key column, there might be (legacy) grouping info
            final GroupingColumnInfo groupingColumnInfo = groupingColumns.get(parquetKeyColumnNames[0]);
            if (groupingColumnInfo != null) {
                return new IndexFileMetadata(
                        makeRelativeURI(parentFileURI, groupingColumnInfo.groupingTablePath()),
                        groupingColumnInfo,
                        null);
            }
        }

        // Either there are more than 1 key columns, or there was no grouping info, so lets see if there was a
        // DataIndex.
        final DataIndexInfo dataIndexInfo = tableInfo.dataIndexes().stream()
                .filter(item -> item.matchesColumns(parquetKeyColumnNames))
                .findFirst()
                .orElse(null);

        if (dataIndexInfo != null) {
            return new IndexFileMetadata(
                    makeRelativeURI(parentFileURI, dataIndexInfo.indexTablePath()),
                    null,
                    dataIndexInfo);
        }

        // We have no index metadata. We intentionally do not fall back to the legacy path from pre-metadata versions
        // of this code, as it's not expected that such tables exist in the wild.
        return null;
    }

    // region Indexing
    /**
     * Read a Data Index table from the disk.
     *
     * @param parentFileURI The path to the base table
     * @param indexFileMetaData Index file metadata
     * @param keyColumnNames The Deephaven names of the key columns
     *
     * @return The data index table for the specified key columns or {@code null} if none was found
     */
    @Nullable
    private Table readDataIndexTable(
            @NotNull final URI parentFileURI,
            @NotNull final ParquetTableLocation.IndexFileMetadata indexFileMetaData,
            @NotNull final String[] keyColumnNames) {
        final boolean isDataIndex = indexFileMetaData.dataIndexInfo != null;
        // The index table's own columns -- the row set column, and a grouping table's key and position columns -- are
        // not columns of the parent table, so no instruction the parent's instructions carry for a column with the same
        // Deephaven name applies to them; they decode with what the index file's footer records.
        final ParquetInstructions.Builder builder = new ParquetInstructions.Builder(readInstructions)
                .setFileLayout(ParquetInstructions.ParquetFileLayout.SINGLE_FILE);
        if (isDataIndex) {
            builder.removeColumnInstructions(INDEX_ROW_SET_COLUMN_NAME);
        } else {
            builder.removeColumnInstructions(GROUPING_KEY_COLUMN_NAME)
                    .removeColumnInstructions(GROUPING_BEGIN_POS_COLUMN_NAME)
                    .removeColumnInstructions(GROUPING_END_POS_COLUMN_NAME);
        }
        final ParquetInstructions indexReadInstructions;
        if (readInstructions.getTableDefinition().isEmpty()) {
            // A location built without a definition can't have a column resolver in its instructions, since setting a
            // resolver requires a definition. Infer the index table's definition, with the same renames as the parent
            // table for the key columns.
            if (isDataIndex) {
                // The row set column's parquet name is found in the index file's schema
                final String rowSetParquetColumnName = resolver.findRowSetParquetColumnName(
                        new ParquetTableLocationKey(indexFileMetaData.fileURI, 0, null, readInstructions)
                                .getFileReader().getSchema(),
                        keyColumnNames);
                if (!INDEX_ROW_SET_COLUMN_NAME.equals(rowSetParquetColumnName)) {
                    // A parent column read from a parquet column with that name is not a column of the index table
                    final String mappedFrom =
                            readInstructions.getColumnNameFromParquetColumnName(rowSetParquetColumnName);
                    if (mappedFrom != null) {
                        builder.removeColumnInstructions(mappedFrom);
                    }
                    builder.addColumnNameMapping(rowSetParquetColumnName, INDEX_ROW_SET_COLUMN_NAME);
                }
            }
            indexReadInstructions = builder.build();
        } else {
            // Each key column is read with the definition of the column it indexes, so the keys have that column's type
            final TableDefinition indexDefinition = isDataIndex
                    ? resolver.dataIndexDefinition(keyColumnNames)
                    : resolver.groupingDefinition(keyColumnNames[0]);
            if (indexDefinition == null) {
                // A key column the definition lacks cannot be given its column's type
                return null;
            }
            // Find the index table's columns with a column resolver: for a data index, a resolver derived from the
            // parent table's, and for a legacy grouping table, a resolver for the grouping table's fixed column names.
            // Either resolver ignores any renames the parent's instructions carry.
            final ParquetColumnResolver.Factory indexColumnResolverFactory;
            if (isDataIndex) {
                // The row set column's parquet name is found in the index file's schema. The key columns have the
                // Deephaven names of the columns they index, so the parent's instructions for those columns, such as
                // codecs, apply to the key columns as they are.
                indexColumnResolverFactory = (tableKey, indexLocationKey) -> Objects.requireNonNull(
                        resolver.forDataIndex(indexLocationKey.getFileReader().getSchema(), keyColumnNames));
            } else {
                indexColumnResolverFactory =
                        (tableKey, indexLocationKey) -> ParquetTableLocationColumnResolver.FOR_GROUPING;
                // The key column is not named after the column it indexes, so the parent's instructions for the indexed
                // column are copied to the key column's name
                final String keyCodecName = readInstructions.getCodecName(keyColumnNames[0]);
                if (keyCodecName != null) {
                    builder.addColumnCodec(GROUPING_KEY_COLUMN_NAME, keyCodecName,
                            readInstructions.getCodecArgs(keyColumnNames[0]));
                }
                readInstructions.getUnsignedLongTarget(keyColumnNames[0])
                        .ifPresent(target -> builder.setUnsignedLongTarget(GROUPING_KEY_COLUMN_NAME, target));
            }
            indexReadInstructions = builder
                    .setColumnResolverFactory(indexColumnResolverFactory)
                    .setTableDefinition(indexDefinition)
                    .build();
        }
        if (isDataIndex) {
            return ParquetTools.readTable(indexFileMetaData.fileURI.toString(), indexReadInstructions);
        }
        Assert.neqNull(indexFileMetaData.groupingColumnInfo, "indexFileMetaData.groupingColumnInfo");
        // Check the format first, since a definition would read a missing column as nulls
        final MessageType groupingSchema =
                new ParquetTableLocationKey(indexFileMetaData.fileURI, 0, null, indexReadInstructions)
                        .getFileReader().getSchema();
        if (!groupingSchema.containsField(GROUPING_KEY_COLUMN_NAME)
                || !groupingSchema.containsField(GROUPING_BEGIN_POS_COLUMN_NAME)
                || !groupingSchema.containsField(GROUPING_END_POS_COLUMN_NAME)) {
            throw new TableDataException(String.format(
                    "Index table %s for table %s was not in the expected format. Expected columns [%s] but encountered [%s]",
                    indexFileMetaData.fileURI, parentFileURI,
                    String.join(", ",
                            GROUPING_KEY_COLUMN_NAME, GROUPING_BEGIN_POS_COLUMN_NAME, GROUPING_END_POS_COLUMN_NAME),
                    groupingSchema.getFields().stream().map(Type::getName).collect(Collectors.joining(", "))));
        }
        // Legacy grouping tables are written with a key, start position, and end position. We must convert the ranges
        // to RowSets. That comes first: a view's columns apply in order, and the key column may be named like a
        // position column.
        return ParquetTools.readTable(indexFileMetaData.fileURI.toString(), indexReadInstructions)
                .updateView(List.of(
                        // Using this lets us avoid a compilation or boxing, but does force us to do single-cell access
                        // rather than using chunks.
                        new MultiSourceFunctionalColumn<>(
                                List.of(GROUPING_BEGIN_POS_COLUMN_NAME, GROUPING_END_POS_COLUMN_NAME),
                                INDEX_ROW_SET_COLUMN_NAME,
                                RowSet.class,
                                (final long rowKey, final ColumnSource<?>[] sources) -> {
                                    final long beginPos = sources[0].getLong(rowKey);
                                    final long endPos = sources[1].getLong(rowKey);
                                    // The end position is exclusive, so an empty group's begin and end are equal.
                                    return endPos <= beginPos
                                            ? RowSetFactory.empty()
                                            : RowSetFactory.fromRange(beginPos, endPos - 1);
                                })))
                .view(List.of(
                        new SourceColumn(GROUPING_KEY_COLUMN_NAME, keyColumnNames[0]),
                        new SourceColumn(INDEX_ROW_SET_COLUMN_NAME)));
    }

    // endregion Indexing

    // region Pushdown Filtering

    private static final RegionedPushdownAction.Location ROW_GROUP_METADATA =
            new RegionedPushdownAction.Location(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA,
                    PushdownResult.REGION_METADATA_STATS_COST,
                    BasePushdownFilterContext::supportsMetadataFiltering,
                    (tl, cr) -> ((ParquetTableLocation) tl).supportsMetadataFiltering());

    private static final RegionedPushdownAction.Location IN_MEMORY_DATA_INDEX =
            new RegionedPushdownAction.Location(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX,
                    PushdownResult.LOCATION_IN_MEMORY_DATA_INDEX_COST,
                    BasePushdownFilterContext::supportsInMemoryDataIndexFiltering,
                    (tl, cr) -> ((ParquetTableLocation) tl).supportsInMemoryDataIndexFiltering());

    private static final RegionedPushdownAction.Location PARQUET_DICTIONARY =
            new RegionedPushdownAction.Location(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_DICTIONARY,
                    PushdownResult.REGION_DICTIONARY_DATA_COST,
                    BasePushdownFilterContext::supportsChunkFiltering,
                    (tl, cr) -> ((ParquetTableLocation) tl).supportsDictionaryFiltering());

    private static final RegionedPushdownAction.Location DEFERRED_DATA_INDEX =
            new RegionedPushdownAction.Location(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX,
                    PushdownResult.LOCATION_DEFERRED_DATA_INDEX_COST,
                    BasePushdownFilterContext::supportsDeferredDataIndexFiltering,
                    (tl, cr) -> ((ParquetTableLocation) tl).supportsDeferredDataIndexFiltering());

    private static final List<RegionedPushdownAction> supportedActions = List.of(
            ROW_GROUP_METADATA,
            IN_MEMORY_DATA_INDEX,
            PARQUET_DICTIONARY,
            DEFERRED_DATA_INDEX);

    @Override
    public List<RegionedPushdownAction> supportedActions() {
        return supportedActions;
    }

    public static class EstimateContext implements RegionedPushdownAction.EstimateContext {
        enum ResolveState {
            RESOLVED, FAILED
        }

        private final ResolveState resolveState;
        /**
         * The table definition's names for the filter's columns, which the filter itself may name differently; see
         * {@link RegionedPushdownFilterContext#filterColumnToManagerColumnName()}.
         */
        private final String[] columnNames;

        private EstimateContext(
                final ResolveState resolveState,
                final String[] columnNames) {
            this.resolveState = resolveState;
            this.columnNames = columnNames;
        }

        @Override
        public void close() {}
    }

    @Override
    public RegionedPushdownAction.EstimateContext makeEstimateContext(
            final WhereFilter filter,
            final PushdownFilterContext filterContext) {
        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) filterContext;

        // We must have an initialized location to create this estimate context.
        initialize();

        final Optional<List<ResolvedColumnInfo>> maybeResolvedColumns =
                resolveColumns(filter, filterCtx.filterColumnToManagerColumnName());
        if (maybeResolvedColumns.isEmpty()) {
            return new EstimateContext(EstimateContext.ResolveState.FAILED, null);
        }

        final String[] columnNames = maybeResolvedColumns.get().stream()
                .map(resolvedColumn -> resolvedColumn.columnName)
                .toArray(String[]::new);
        return new EstimateContext(EstimateContext.ResolveState.RESOLVED, columnNames);
    }

    @Override
    public long estimatePushdownAction(
            final RegionedPushdownAction action,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.EstimateContext estimateContext) {
        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) filterContext;
        final EstimateContext estimateCtx = (EstimateContext) estimateContext;

        if (estimateCtx.resolveState == EstimateContext.ResolveState.FAILED) {
            // One or more columns could not be resolved, so no benefit to pushing down.
            return PushdownResult.UNSUPPORTED_ACTION_COST;
        }

        // Apply a more specific check that depends on materializing parquet metadata
        final boolean isApplicable;
        if (action == ROW_GROUP_METADATA) {
            // Note: it should be possible to check if there are any statistics
            isApplicable = true;
        } else if (action == IN_MEMORY_DATA_INDEX) {
            isApplicable = hasCachedDataIndex(estimateCtx.columnNames);
        } else if (action == PARQUET_DICTIONARY) {
            isApplicable = ((ParquetColumnLocation<?>) getColumnLocation(estimateCtx.columnNames[0]))
                    .mayHaveDictionaryPages(filterCtx.columnDefinitions().get(0));
        } else if (action == DEFERRED_DATA_INDEX) {
            isApplicable = hasDataIndex(estimateCtx.columnNames);
        } else {
            // TODO(DH-19666): Add support for bloom filters, sortedness, etc.
            return PushdownResult.UNSUPPORTED_ACTION_COST;
        }

        return isApplicable ? action.filterCost() : PushdownResult.UNSUPPORTED_ACTION_COST;
    }

    public static class ActionContext implements RegionedPushdownAction.ActionContext {
        enum ResolveState {
            RESOLVED, FAILED
        }

        private final ResolveState resolveState;
        /**
         * The table definition's names for the filter's columns, which the filter itself may name differently; see
         * {@link RegionedPushdownFilterContext#filterColumnToManagerColumnName()}.
         */
        private final String[] columnNames;
        /** The indices of the filter's columns in the file schema's leaf columns, for finding their statistics. */
        private final List<Integer> columnIndices;

        private ActionContext(
                final ResolveState resolveState,
                final String[] columnNames,
                final List<Integer> columnIndices) {
            this.resolveState = resolveState;
            this.columnNames = columnNames;
            this.columnIndices = columnIndices;
        }

        @Override
        public void close() {}
    }

    @Override
    public RegionedPushdownAction.ActionContext makeActionContext(
            final WhereFilter filter,
            final PushdownFilterContext filterContext) {
        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) filterContext;

        // We must have an initialized location to create this action context.
        initialize();

        final Map<String, String> renameMap = filterCtx.filterColumnToManagerColumnName();
        final Optional<List<ResolvedColumnInfo>> maybeResolvedColumns = resolveColumns(filter, renameMap);
        if (maybeResolvedColumns.isEmpty()) {
            return new ActionContext(ActionContext.ResolveState.FAILED, null, null);
        }

        final List<ResolvedColumnInfo> resolvedColumnsInfo = maybeResolvedColumns.get();

        final int numColumns = resolvedColumnsInfo.size();
        final String[] columnNames = new String[numColumns];
        final List<Integer> columnIndices = new ArrayList<>(numColumns);

        for (int i = 0; i < numColumns; i++) {
            final ResolvedColumnInfo resolvedColumn = resolvedColumnsInfo.get(i);
            columnNames[i] = resolvedColumn.columnName;
            columnIndices.add(resolvedColumn.columnIndex);
        }
        return new ActionContext(ActionContext.ResolveState.RESOLVED, columnNames, columnIndices);
    }

    @Override
    public PushdownResult performPushdownAction(
            final RegionedPushdownAction action,
            final WhereFilter filter,
            final RowSet selection,
            final PushdownResult input,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.ActionContext actionContext) {
        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) filterContext;
        final ActionContext actionCtx = (ActionContext) actionContext;

        if (actionCtx.resolveState == ActionContext.ResolveState.FAILED) {
            // One or more columns could not be resolved, so return the input
            return input.copy();
        }

        if (action == ROW_GROUP_METADATA) {
            return pushdownRowGroupMetadata(selection, filterCtx, actionCtx, input);
        }
        if (action == IN_MEMORY_DATA_INDEX) {
            final BasicDataIndex dataIndex =
                    hasCachedDataIndex(actionCtx.columnNames) ? getDataIndex(actionCtx.columnNames) : null;
            if (dataIndex == null) {
                return input.copy();
            }
            return pushdownDataIndex(selection, filter, filterCtx.filterColumnToManagerColumnName(), dataIndex, input);
        }
        if (action == PARQUET_DICTIONARY) {
            if (!((ParquetColumnLocation<?>) getColumnLocation(actionCtx.columnNames[0]))
                    .mayHaveDictionaryPages(filterCtx.columnDefinitions().get(0))) {
                return input.copy();
            }
            return pushdownFilterDictionary(selection, filterCtx, actionCtx.columnNames, input);
        }
        if (action == DEFERRED_DATA_INDEX) {
            final BasicDataIndex dataIndex =
                    hasDataIndex(actionCtx.columnNames) ? getDataIndex(actionCtx.columnNames) : null;
            if (dataIndex == null) {
                return input.copy();
            }
            return pushdownDataIndex(selection, filter, filterCtx.filterColumnToManagerColumnName(), dataIndex, input);
        }
        throw new IllegalStateException("Unexpected value: " + action);
    }

    /**
     * A helper class to hold a filter column's name in the table definition and the index of the parquet column it
     * reads.
     */
    private static class ResolvedColumnInfo {
        /**
         * The column's name in the table definition, which is what {@link #getColumnLocation}, the data index methods
         * and the other {@link TableLocation} APIs expect.
         */
        final String columnName;
        final int columnIndex;

        ResolvedColumnInfo(
                @NotNull final String columnName,
                final int columnIndex) {
            this.columnName = columnName;
            this.columnIndex = columnIndex;
        }
    }

    /**
     * Checks if the column is supported for pushdown filtering.
     */
    private boolean isSupportedForPushdown(
            @NotNull final String colNameFromDef,
            @NotNull final List<String> columnPath) {
        // Only flat columns are supported for push-down
        if (columnPath.size() != 1) {
            return false;
        }
        final String parquetColumnName = columnPath.get(0);
        if (!parquetSchema.containsField(parquetColumnName)) {
            // Column not found in the schema
            return false;
        }
        final Type parquetType = parquetSchema.getType(parquetColumnName);
        if (!parquetType.isPrimitive()) {
            // Cannot push down filters on group types
            return false;
        }
        if (parquetType.isRepetition(Type.Repetition.REPEATED)) {
            // A repeated column's statistics describe leaf values rather than rows -- one row spans many of them, and
            // num_nulls counts leaf nulls, not null rows. Neither can be read as a statement about rows, so decline
            // rather than interpret them. Deephaven never writes such a column (arrays and vectors are written as
            // nested LIST groups, which the path check above rejects), but other writers do.
            return false;
        }
        if (parquetType.asPrimitiveType().columnOrder() != ColumnOrder.typeDefined()) {
            // We only handle typeDefined min/max right now; if new orders get defined in the future, they need to
            // be explicitly handled
            return false;
        }

        // Should not have a codec defined in the instructions or footer metadata
        final String codecFromInstructions = readInstructions.getCodecName(colNameFromDef);
        final ColumnTypeInfo columnTypeInfo = getColumnTypes().get(parquetColumnName);
        final Object codec = codecFromInstructions != null ? codecFromInstructions
                : columnTypeInfo == null ? null : columnTypeInfo.codec().orElse(null);
        if (codec != null) {
            return false;
        }

        // Should not have a special type defined in the instructions or footer metadata
        final ColumnTypeInfo.SpecialType specialType =
                columnTypeInfo == null ? null : columnTypeInfo.specialType().orElse(null);
        return specialType == null;
    }

    /**
     * Returns the index of the top-level parquet column {@code parquetColumnName} in {@code parquetColumnPaths}, or
     * {@link OptionalInt#empty()} if the column is absent. This index is used to find the statistics for a column when
     * pushing down filters.
     */
    private static OptionalInt findColumnIndex(
            @NotNull final String parquetColumnName,
            @NotNull final List<String[]> parquetColumnPaths) {
        for (int i = 0; i < parquetColumnPaths.size(); ++i) {
            final String[] path = parquetColumnPaths.get(i);
            if (path.length == 1 && path[0].equals(parquetColumnName)) {
                return OptionalInt.of(i);
            }
        }
        return OptionalInt.empty();
    }

    /**
     * Attempts to resolve all columns referenced by {@code filter} against the Parquet schema.
     *
     * @param filter The filter containing the columns to resolve
     * @param renameMap A map from the filter's column names to the table definition's names for them; a name it lacks
     *        is the same in both
     * @return {@code Optional.empty()} if <b>any</b> column cannot be resolved, otherwise an {@code Optional}
     *         containing the fully resolved list.
     */
    private Optional<List<ResolvedColumnInfo>> resolveColumns(
            @NotNull final WhereFilter filter,
            @NotNull final Map<String, String> renameMap) {
        final Collection<String> colNamesFromFilter = filter.getColumns();
        final List<ResolvedColumnInfo> resolvedColumns = new ArrayList<>(colNamesFromFilter.size());
        final List<String[]> pathsFromSchema = ParquetSchemaUtil.paths(parquetSchema);
        for (final String colNameFromFilter : colNamesFromFilter) {
            final String colNameFromDef = renameMap.getOrDefault(colNameFromFilter, colNameFromFilter);
            final List<String> columnPath = getColumnPath(colNameFromDef);
            if (!isSupportedForPushdown(colNameFromDef, columnPath)) {
                return Optional.empty();
            }

            // Assuming a non-nested column, the first element of the column path is the parquet column name
            final String parquetColumnName = columnPath.get(0);
            final OptionalInt columnIndex = findColumnIndex(parquetColumnName, pathsFromSchema);
            if (columnIndex.isEmpty()) {
                // Column not found in the schema
                return Optional.empty();
            }
            resolvedColumns.add(new ResolvedColumnInfo(colNameFromDef, columnIndex.getAsInt()));
        }
        return Optional.of(resolvedColumns);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The following should be _cheap_ checks that don't require materializing Parquet metadata to check.
    // ---------------------------------------------------------------------------------------------------------------
    // Note: in the future, we this would be an easy way to allow turning off Parquet pushdown on a location by location
    // basis; we could expose the options through ParquetInstructions

    private boolean supportsMetadataFiltering() {
        return true;
    }

    private boolean supportsDictionaryFiltering() {

        return true;
    }

    private boolean supportsInMemoryDataIndexFiltering() {
        return hasAnyCachedDataIndex();
    }

    private boolean supportsDeferredDataIndexFiltering() {
        return true;
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Consumer for row groups and row sets.
     */
    private interface RowGroupAndRowSetConsumer {
        void accept(int rowGroupIndex, RowSequence rs);
    }

    /**
     * Iterate over the row groups and the matching row sets, calling the consumer for each row group and row set.
     */
    private void iterateRowGroupsAndRowSet(final RowSet input, final RowGroupAndRowSetConsumer consumer) {
        try (final RowSequence.Iterator rsIt = input.getRowSequenceIterator()) {
            final RowGroupReader[] rgReaders = getRowGroupReaders();
            for (int rgIdx = 0; rgIdx < rgReaders.length; rgIdx++) {
                final long subRegionSize = rgReaders[rgIdx].getRowGroup().getNum_rows();
                final long subRegionFirstKey = getSubRegionFirstKey(rgIdx);
                final long subRegionLastKey = subRegionFirstKey + subRegionSize - 1;

                final RowSequence rs = rsIt.getNextRowSequenceThrough(subRegionLastKey);
                if (rs.isEmpty()) {
                    continue;
                }
                consumer.accept(rgIdx, rs);
            }
        }
    }

    /**
     * Apply the filter to the row group metadata and return the result.
     */
    @NotNull
    private PushdownResult pushdownRowGroupMetadata(
            final RowSet selection,
            final RegionedPushdownFilterContext ctx,
            final ActionContext actionCtx,
            final PushdownResult result) {
        final WhereFilter filter = ctx.filterForMetadataFiltering();
        final RowSetBuilderSequential maybeBuilder = RowSetFactory.builderSequential();
        final MutableLong maybeCount = new MutableLong(0);

        // Only one column in these filters
        final Integer columnIndex = actionCtx.columnIndices.get(0);

        // Resolve the filter against the column type once, not once per row group. Everything that depends only on
        // the filter -- unboxing its values into a primitive array, encoding them, deciding whether the type is
        // supported at all -- happens here, and the loop below is left with just the statistics.
        //
        // TODO (DH-19666): Hoist this to the filter context. The evaluator is a pure function of (filter, ctx), and
        // both are per-filter rather than per-location -- ctx is created once in AbstractFilterExecution and shared
        // across locations -- so nothing here depends on this location. As written the unboxing and encoding above
        // are repeated once per location, which for a large match filter over many partitions is a real cost:
        // encoding 10,000 string values measures around 370us, so 1,000 locations spend a third of a second of CPU
        // rebuilding identical evaluators. Memoizing the evaluator on the RegionedPushdownFilterContext would build
        // it once for the whole filter.
        final StatisticsEvaluator evaluator = StatisticsEvaluator.makeForFilter(filter, ctx);
        if (evaluator == StatisticsEvaluator.ALWAYS_MAYBE) {
            // Nothing about this filter can be bounded by statistics, so every row group would be kept.
            return result.copy();
        }

        // The block list is file-wide, and for a _metadata layout covers every file in the dataset, so it is indexed by
        // the row group's index in the file rather than by its position within this location.
        final List<BlockMetaData> blocks = parquetMetadata.getBlocks();
        iterateRowGroupsAndRowSet(result.maybeMatch(), (rgIdx, rs) -> {
            final Statistics<?> statistics =
                    blocks.get(rowGroupIndices[rgIdx]).getColumns().get(columnIndex).getStatistics();
            // TODO (DH-19666) Right now, the pushdown logic only returns maybeMatch for row group. For the future, we
            // can return "match" for scenarios like filter of {X == 3}, and statistics of {min=3, max=3, num_nulls=0}.
            // Similarly, if filter is {X == null}, and statistics is {hasNonNullValue=false, num_nulls=<row-group
            // size>}, we can return "match" for the row group.
            //
            // Statistics this code cannot use keep the row group; the evaluator applies that check itself, so there is
            // nothing to screen for here. See UsabilityEvaluator.
            final boolean maybeOverlaps = evaluator.maybeOverlaps(statistics);
            if (maybeOverlaps) {
                maybeBuilder.appendRowSequence(rs);
                maybeCount.add(rs.size());
            }
        });
        if (maybeCount.get() == result.maybeMatch().size()) {
            return result.copy();
        }
        try (final WritableRowSet maybeMatch = maybeBuilder.build()) {
            return PushdownResult.of(selection, result.match(), maybeMatch);
        }
    }

    /**
     * Apply the chunk filter to the row group dictionaries and return the result.
     */
    @NotNull
    private PushdownResult pushdownFilterDictionary(
            final RowSet selection,
            final RegionedPushdownFilterContext ctx,
            final String[] columnNames,
            final PushdownResult result) {

        final BasePushdownFilterContext.FilterNullBehavior filterNullBehavior = ctx.filterNullBehavior();
        if (filterNullBehavior == BasePushdownFilterContext.FilterNullBehavior.FAILS_ON_NULLS) {
            // We cannot use dictionary-based filtering, keep all the rows as "maybe" rows. Later when the
            // filter is applied to actual data, it will NPE if there are null values in the table.
            // Future optimization: Use the row-group statistics to determine if there are any nulls in the column
            return result.copy();
        }

        final RowSetBuilderSequential matchBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential maybeBuilder = RowSetFactory.builderSequential();
        final MutableLong maybeCount = new MutableLong(0);

        final ColumnDefinition<?> columnDefinition = ctx.columnDefinitions().get(0);

        // noinspection unchecked
        final ParquetColumnLocation<Values> columnLocation =
                (ParquetColumnLocation<Values>) getColumnLocation(columnNames[0]);

        // Get the dictionary chunks for the row groups.
        // noinspection unchecked
        final Chunk<Values>[] dictionaryChunks =
                Arrays.stream(columnLocation.getDictionaryChunkSuppliers(columnDefinition))
                        .map(supplier -> supplier == null ? null : supplier.get())
                        .toArray(Chunk[]::new);

        final int maxSize = Arrays.stream(dictionaryChunks)
                .mapToInt(chunk -> chunk == null ? 0 : chunk.size())
                .max()
                .orElse(0);
        if (maxSize == 0) {
            // No dictionaries of non-zero size, nothing to do.
            return result.copy();
        }

        // Get the key page stores for the row groups.
        final ColumnChunkPageStore<DictionaryKeys>[] valueStores =
                columnLocation.getDictionaryKeysPageStores(columnDefinition);

        // Allocate a writable chunk sized to the largest dictionary across row groups, and fill it with sequential
        // indices (0..N-1). These values act as candidate dictionary entry IDs.
        try (final WritableLongChunk<OrderedRowKeys> keyCandidates = WritableLongChunk.makeWritableChunk(maxSize);
                final BasePushdownFilterContext.UnifiedChunkFilter chunkFilter = ctx.createChunkFilter(maxSize)) {
            for (int ii = 0; ii < maxSize; ii++) {
                keyCandidates.set(ii, ii);
            }

            // Iterate each row group intersecting the current "maybe" rows.
            iterateRowGroupsAndRowSet(result.maybeMatch(), (rgIdx, rs) -> {

                // Get the dictionary chunk and value store for this row group.
                final Chunk<Values> dictionaryChunk = dictionaryChunks[rgIdx];
                final ColumnChunkPageStore<DictionaryKeys> valueStore = valueStores[rgIdx];

                if (dictionaryChunk == null || !valueStore.usesDictionaryOnEveryPage()) {
                    // This row group does not use the dictionary, keep all the rows as "maybe" rows.
                    maybeBuilder.appendRowSequence(rs);
                    maybeCount.add(rs.size());
                    return;
                }

                // Filtering the dictionary costs O(dictionary size), might not be worth the overhead.
                final long threshold = (long) (rs.size() * QueryTable.DICTIONARY_FOR_WHERE_THRESHOLD);
                if (dictionaryChunk.size() >= threshold) {
                    maybeBuilder.appendRowSequence(rs);
                    maybeCount.add(rs.size());
                    return;
                }

                // Run the filter on the dictionary to find which dictionary entries satisfy the filter and build an
                // array of matching dictionary key IDs.
                final long[] keyMatchArray;
                try (final WritableLongChunk<OrderedRowKeys> keyCandidatesForChunk =
                        keyCandidates.slice(0, dictionaryChunk.size())) {
                    final LongChunk<OrderedRowKeys> keyMatch =
                            chunkFilter.filter(dictionaryChunk, keyCandidatesForChunk);
                    if (filterNullBehavior == BasePushdownFilterContext.FilterNullBehavior.INCLUDES_NULLS) {
                        // Include one extra slot for NULL_LONG as a matching key if the filter includes nulls.
                        keyMatchArray = new long[keyMatch.size() + 1];
                        keyMatchArray[keyMatch.size()] = NULL_LONG;
                    } else if (filterNullBehavior == BasePushdownFilterContext.FilterNullBehavior.EXCLUDES_NULLS) {
                        if (keyMatch.size() == 0) {
                            // No matches, and nulls are excluded, so all rows in this row group are excluded.
                            return;
                        }
                        keyMatchArray = new long[keyMatch.size()];
                    } else {
                        throw new IllegalStateException("Unexpected null behavior: " + filterNullBehavior);
                    }
                    keyMatch.copyToTypedArray(0, keyMatchArray, 0, keyMatch.size());
                }

                // Make a MatchFilter with the matching dictionary key IDs. This will accept any encoded value whose
                // dictionary index is in keyMatchArray
                final ChunkFilter matchChunkFilter =
                        LongChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR, keyMatchArray);

                // Now we need to apply this filter to the encoded values in the row group. We can do this by
                // iterating the "maybe" rows in chunks, getting the encoded values for those rows, and applying the
                // filter to those encoded values.
                // We must shift the row keys to be relative to the row group because the value store is relative to the
                // row group.
                final long subRegionFirstKey = getSubRegionFirstKey(rgIdx);
                final int CHUNK_SIZE = 4096;
                try (final RowSet rowSet = rs.asRowSet();
                        final RowSet shiftedRowSet = rowSet.shift(-subRegionFirstKey);
                        final RowSequence.Iterator it = shiftedRowSet.getRowSequenceIterator();
                        final ChunkSource.GetContext getContext = valueStore.makeGetContext(CHUNK_SIZE);
                        final WritableLongChunk<OrderedRowKeys> results =
                                WritableLongChunk.makeWritableChunk(CHUNK_SIZE)) {
                    while (it.hasMore()) {
                        final RowSequence nextRowSeq = it.getNextRowSequenceWithLength(CHUNK_SIZE);
                        final Chunk<? extends DictionaryKeys> valueChunk = valueStore.getChunk(getContext, nextRowSeq);
                        matchChunkFilter.filter(valueChunk, nextRowSeq.asRowKeyChunk(), results);

                        // Iterate over matching row keys, convert them to original row space and save them as a match
                        final int numMatches = results.size();
                        for (int idx = 0; idx < numMatches; idx++) {
                            final long rowKey = results.get(idx);
                            final long originalRowKey = subRegionFirstKey + rowKey;
                            matchBuilder.appendKey(originalRowKey);
                        }
                    }
                }
            });
        }

        try (final WritableRowSet matching = matchBuilder.build();
                final WritableRowSet maybe = maybeCount.get() == result.maybeMatch().size()
                        ? result.maybeMatch().copy()
                        : maybeBuilder.build()) {
            matching.insert(result.match());
            return PushdownResult.of(selection, matching, maybe);
        }
    }

    /**
     * Apply the filter to the data index table and return the result. When the index is applied, the result has no
     * maybe matches: rows the index does not place under a matching key are treated as not matching, so the index must
     * be complete and current for the rows it covers (see {@link #hasDataIndex}). When the selection is too small for
     * the index, or filtering the index fails for a reason other than cancellation, the result is a copy of
     * {@code result}.
     */
    @NotNull
    public static PushdownResult pushdownDataIndex(
            final RowSet selection,
            final WhereFilter filter,
            final Map<String, String> renameMap,
            final BasicDataIndex dataIndex,
            final PushdownResult result) {
        final RowSetBuilderRandom matchingBuilder = RowSetFactory.builderRandom();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final long threshold = (long) (dataIndex.table().size() / QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD);
            if (result.maybeMatch().size() <= threshold) {
                return result.copy();
            }
            // Extract the fundamental filter, ignoring barriers and serial wrappers.
            final WhereFilter copiedFilter = ExtractFilterWithoutBarriers.of(filter).copy();
            final Table toFilter;
            if (!renameMap.isEmpty()) {
                final Collection<Pair> renamePairs = renameMap.entrySet().stream()
                        .map(entry -> io.deephaven.api.Pair.of(ColumnName.of(entry.getValue()),
                                ColumnName.of(entry.getKey())))
                        .collect(Collectors.toList());
                toFilter = dataIndex.table().renameColumns(renamePairs);
            } else {
                toFilter = dataIndex.table();
            }
            // Apply the filter to the data index table
            try {
                final Table filteredTable = toFilter.where(copiedFilter);
                try (final CloseableIterator<RowSet> it =
                        ColumnVectors.ofObject(filteredTable, dataIndex.rowSetColumnName(), RowSet.class).iterator()) {
                    it.forEachRemaining(rowSet -> {
                        try (final RowSet matching = rowSet.intersect(result.maybeMatch())) {
                            matchingBuilder.addRowSet(matching);
                        }
                    });
                }
            } catch (final Exception e) {
                // A cancelled query must stop, not carry on filtering without the index.
                if (CancellationException.isCancellation(e)) {
                    throw e;
                }
                // The index's key columns are read as the columns they index, renames and type conversions included,
                // but filtering the index can still fail, for instance when its file can't be read as the indexed
                // column's type. Leave every row to the filter itself.
                log.warn().append("Skipping data index pushdown for filter ").append(String.valueOf(filter))
                        .append(": ").append(String.valueOf(e)).endl();
                return result.copy();
            }
        }
        // Retain only the maybe rows and add the previously found matches.
        try (
                final WritableRowSet matching = matchingBuilder.build();
                final WritableRowSet empty = RowSetFactory.empty()) {
            matching.insert(result.match());
            return PushdownResult.of(selection, matching, empty);
        }
    }

    // endregion Pushdown Filtering
}

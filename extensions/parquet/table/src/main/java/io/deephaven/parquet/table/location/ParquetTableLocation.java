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
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
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
import io.deephaven.engine.table.impl.dataindex.StandaloneDataIndex;
import io.deephaven.engine.table.impl.filter.ExtractFilterWithoutBarriers;
import io.deephaven.engine.table.impl.filter.ExtractRespectedBarriers;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
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
import io.deephaven.parquet.table.metadata.SortColumnInfo;
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
import java.util.function.Supplier;
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
    private ParquetColumnResolver resolver;

    private RegionedPageStore.Parameters regionParameters;
    private Map<String, String[]> parquetColumnNameToPath;

    private TableInfo tableInfo;
    private Map<String, GroupingColumnInfo> groupingColumns;
    private Map<String, ColumnTypeInfo> columnTypes;
    private List<SortColumn> sortingColumns;

    private ParquetFileReader parquetFileReader;
    private ParquetMetadata parquetMetadata;
    private int[] rowGroupIndices;
    private MessageType parquetSchema;
    // -----------------------------------------------------------------------

    private volatile RowGroupReader[] rowGroupReaders;

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
                rowGroupIndices = tableLocationKey.getRowGroupIndices();
            }

            resolver = readInstructions.getColumnResolverFactory()
                    .map(factory -> factory.of(getTableKey(), tableLocationKey))
                    .orElse(null);
            final int rowGroupCount = rowGroupIndices.length;
            final RowGroup[] rowGroups = IntStream.of(rowGroupIndices)
                    .mapToObj(rgi -> parquetFileReader.fileMetaData.getRow_groups().get(rgi))
                    .sorted(Comparator.comparingInt(RowGroup::getOrdinal))
                    .toArray(RowGroup[]::new);
            final long maxRowCount = Arrays.stream(rowGroups).mapToLong(RowGroup::getNum_rows).max().orElse(0L);
            regionParameters = new RegionedPageStore.Parameters(
                    RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, rowGroupCount, maxRowCount);

            parquetSchema = parquetFileReader.getSchema();

            parquetColumnNameToPath = new HashMap<>();
            for (String[] path : ParquetSchemaUtil.paths(parquetSchema)) {
                if (path.length > 1) {
                    parquetColumnNameToPath.put(path[0], path);
                }
            }

            tableInfo = ParquetSchemaReader
                    .parseMetadata(parquetMetadata.getFileMetaData().getKeyValueMetaData())
                    .orElse(TableInfo.builder().build());
            groupingColumns = tableInfo.groupingColumnMap();
            columnTypes = tableInfo.columnTypeMap();
            sortingColumns = SortColumnInfo.sortColumns(tableInfo.sortingColumns());

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

    public Map<String, ColumnTypeInfo> getColumnTypes() {
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
                    .sorted(Comparator.comparingInt(rgr -> rgr.getRowGroup().getOrdinal()))
                    .toArray(RowGroupReader[]::new);

            // We don't need these anymore
            parquetFileReader = null;
            rowGroupIndices = null;

            rowGroupReaders = local;
            return local;
        }
    }

    @Override
    @NotNull
    public List<SortColumn> getSortedColumns() {
        initialize();
        return sortingColumns;
    }

    @Override
    protected final void initializeState() {
        initialize();
    }

    @Override
    @NotNull
    protected ColumnLocation makeColumnLocation(@NotNull final String columnName) {
        final String parquetColumnName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName);
        return new ParquetColumnLocation<>(this, columnName, parquetColumnName);
    }

    List<String> getColumnPath(@NotNull String columnName, String parquetColumnNameOrDefault) {
        initialize();
        // In the future, we could make this more granular so that the resolver can be constructed without calling
        // initialize first.
        if (resolver != null) {
            // empty list will result in exists=false
            return resolver.of(columnName).orElse(List.of());
        }
        final String[] columnPath = parquetColumnNameToPath.get(parquetColumnNameOrDefault);
        // noinspection Java9CollectionFactory
        return columnPath == null
                ? Collections.singletonList(parquetColumnNameOrDefault)
                : Collections.unmodifiableList(Arrays.asList(columnPath));
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
        // Add the data indexes to the list
        tableInfo.dataIndexes().stream()
                .map(di -> di.columns().toArray(String[]::new))
                .forEach(dataIndexColumns::add);
        // Add grouping columns to the list
        groupingColumns.keySet().stream().map(colName -> new String[] {colName}).forEach(dataIndexColumns::add);
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
        // Check if the column name matches any of the grouping columns
        if (columns.length == 1 && groupingColumns.containsKey(columns[0])) {
            // Validate the index file exists (without loading and parsing it)
            final IndexFileMetadata metadata = getIndexFileMetadata(getParquetKey().getURI(), columns);
            return metadata != null && parquetFileExists(metadata.fileURI);
        }
        // Check if the column names match any of the data indexes
        for (final DataIndexInfo dataIndex : tableInfo.dataIndexes()) {
            if (dataIndex.matchesColumns(columns)) {
                // Validate the index file exists (without loading and parsing it)
                final IndexFileMetadata metadata = getIndexFileMetadata(getParquetKey().getURI(), columns);
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
        final IndexFileMetadata indexFileMetaData = getIndexFileMetadata(getParquetKey().getURI(), columns);
        if (indexFileMetaData == null) {
            throw new TableDataException(
                    String.format(
                            "No index metadata for table %s with index key columns %s was present in TableInfo",
                            getParquetKey().getURI(), Arrays.toString(columns)));
        }
        // Create a new index from the parquet table
        final Table table = readDataIndexTable(getParquetKey().getURI(), indexFileMetaData, readInstructions);
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
            return new RawTableStandaloneDataIndex(adjustedTable, columns, table.coalesce());
        } catch (final RuntimeException | Error e) {
            SafeCloseable.closeAllDuringFailure(e, locationRowSet);
            throw e;
        }
    }

    /**
     * An extension of {@link StandaloneDataIndex} that allows direct access to the underlying index table, whose row
     * sets are positions within the location rather than row keys.
     */
    private static final class RawTableStandaloneDataIndex extends StandaloneDataIndex {
        /** The index table as stored, with positions within the location in its row set column. */
        private final Table rawTable;

        private RawTableStandaloneDataIndex(
                @NotNull final Table adjustedTable,
                @NotNull final String[] keyColumnNames,
                @NotNull final Table rawTable) {
            super(adjustedTable.coalesce(), keyColumnNames, INDEX_ROW_SET_COLUMN_NAME);
            this.rawTable = rawTable;
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

    @Nullable
    private IndexFileMetadata getIndexFileMetadata(
            @NotNull final URI parentFileURI,
            @NotNull final String... keyColumnNames) {
        if (keyColumnNames.length == 1) {
            // If there's only one key column, there might be (legacy) grouping info
            final GroupingColumnInfo groupingColumnInfo = groupingColumns.get(keyColumnNames[0]);
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
                .filter(item -> item.matchesColumns(keyColumnNames))
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
     * @param parquetInstructions The instructions for reading the table
     *
     * @return The data index table for the specified key columns or {@code null} if none was found
     */
    @Nullable
    private static Table readDataIndexTable(
            @NotNull final URI parentFileURI,
            @NotNull final ParquetTableLocation.IndexFileMetadata indexFileMetaData,
            @NotNull final ParquetInstructions parquetInstructions) {
        final Table indexTable = ParquetTools.readTable(indexFileMetaData.fileURI.toString(),
                parquetInstructions.withTableDefinitionAndLayout(null,
                        ParquetInstructions.ParquetFileLayout.SINGLE_FILE));
        if (indexFileMetaData.dataIndexInfo != null) {
            return indexTable;
        }
        Assert.neqNull(indexFileMetaData.groupingColumnInfo, "indexFileMetaData.groupingColumnInfo");
        if (indexTable.hasColumns(
                GROUPING_KEY_COLUMN_NAME, GROUPING_BEGIN_POS_COLUMN_NAME, GROUPING_END_POS_COLUMN_NAME)) {
            // Legacy grouping tables are written with a key, start position, and end position. We must convert the
            // ranges to RowSets.
            return indexTable.view(List.of(
                    new SourceColumn(GROUPING_KEY_COLUMN_NAME, indexFileMetaData.groupingColumnInfo.columnName()),
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
                            })));
        } else {
            throw new TableDataException(String.format(
                    "Index table %s for table %s was not in the expected format. Expected columns [%s] but encountered [%s]",
                    indexFileMetaData.fileURI, parentFileURI,
                    String.join(", ",
                            GROUPING_KEY_COLUMN_NAME, GROUPING_BEGIN_POS_COLUMN_NAME, GROUPING_END_POS_COLUMN_NAME),
                    indexTable.getDefinition().getColumnNamesAsString()));
        }
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

    /**
     * Checks if the column has a dictionary page.
     *
     * @param parquetColumnName The name of the Parquet column
     * @param columnDefinition The definition of the column (required to access dictionary chunk suppliers)
     * @return {@code true} if the column has a dictionary page, {@code false} otherwise
     */
    private boolean hasDictionaryPage(final String parquetColumnName, final ColumnDefinition<?> columnDefinition) {
        // noinspection unchecked
        final ParquetColumnLocation<Values> columnLocation =
                (ParquetColumnLocation<Values>) getColumnLocation(parquetColumnName);
        final Supplier<Chunk<Values>>[] chunkSuppliers =
                columnLocation.getDictionaryChunkSuppliers(columnDefinition);
        final Chunk<Values> dictionaryChunk;

        return chunkSuppliers != null
                && chunkSuppliers.length > 0
                && chunkSuppliers[0] != null
                && (dictionaryChunk = chunkSuppliers[0].get()) != null
                && dictionaryChunk.size() > 0;
    }

    public static class EstimateContext implements RegionedPushdownAction.EstimateContext {
        enum ResolveState {
            RESOLVED, FAILED
        }

        private final ResolveState resolveState;
        private final String[] parquetColumnNames;

        private EstimateContext(
                final ResolveState resolveState,
                final String[] parquetColumnNames) {
            this.resolveState = resolveState;
            this.parquetColumnNames = parquetColumnNames;
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

        final List<ResolvedColumnInfo> resolvedColumnsInfo = maybeResolvedColumns.get();
        // We have verified these columns are not nested.
        final String[] parquetColumnNames = resolvedColumnsInfo.stream()
                .map(resolvedColumn -> resolvedColumn.columnPath.get(0))
                .toArray(String[]::new);
        return new EstimateContext(EstimateContext.ResolveState.RESOLVED, parquetColumnNames);
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
            isApplicable = hasCachedDataIndex(estimateCtx.parquetColumnNames);
        } else if (action == PARQUET_DICTIONARY) {
            isApplicable = hasDictionaryPage(estimateCtx.parquetColumnNames[0], filterCtx.columnDefinitions().get(0));
        } else if (action == DEFERRED_DATA_INDEX) {
            isApplicable = hasDataIndex(estimateCtx.parquetColumnNames);
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
        private final String[] parquetColumnNames;
        private final List<Integer> columnIndices;

        private ActionContext(
                final ResolveState resolveState,
                final String[] parquetColumnNames,
                final List<Integer> columnIndices) {
            this.resolveState = resolveState;
            this.parquetColumnNames = parquetColumnNames;
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

        // We have verified these columns are not nested.
        final int numColumns = resolvedColumnsInfo.size();
        final String[] parquetColumnNames = new String[numColumns];
        final List<Integer> columnIndices = new ArrayList<>(numColumns);

        for (int i = 0; i < numColumns; i++) {
            final ResolvedColumnInfo resolvedColumn = resolvedColumnsInfo.get(i);
            parquetColumnNames[i] = resolvedColumn.columnPath.get(0);
            columnIndices.add(resolvedColumn.columnIndex);
        }
        return new ActionContext(ActionContext.ResolveState.RESOLVED, parquetColumnNames, columnIndices);
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
                    hasCachedDataIndex(actionCtx.parquetColumnNames) ? getDataIndex(actionCtx.parquetColumnNames)
                            : null;
            if (dataIndex == null) {
                return input.copy();
            }
            return pushdownDataIndex(selection, filter, filterCtx.filterColumnToManagerColumnName(), dataIndex, input);
        }
        if (action == PARQUET_DICTIONARY) {
            if (!hasDictionaryPage(actionCtx.parquetColumnNames[0], filterCtx.columnDefinitions().get(0))) {
                return input.copy();
            }
            return pushdownFilterDictionary(selection, filter, filterCtx, actionCtx.parquetColumnNames, input);
        }
        if (action == DEFERRED_DATA_INDEX) {
            final BasicDataIndex dataIndex =
                    hasDataIndex(actionCtx.parquetColumnNames) ? getDataIndex(actionCtx.parquetColumnNames) : null;
            if (dataIndex == null) {
                return input.copy();
            }
            return pushdownDataIndex(selection, filter, filterCtx.filterColumnToManagerColumnName(), dataIndex, input);
        }
        throw new IllegalStateException("Unexpected value: " + action);
    }

    /**
     * A helper class to hold the resolved column paths and their corresponding column indices.
     */
    private static class ResolvedColumnInfo {
        final List<String> columnPath;
        final int columnIndex;

        ResolvedColumnInfo(
                @NotNull final List<String> columnPath,
                final int columnIndex) {
            this.columnPath = columnPath;
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
        final String columnNameInSchema = columnPath.get(0);
        if (!parquetSchema.containsField(columnNameInSchema)) {
            // Column not found in the schema
            return false;
        }
        final Type parquetType = parquetSchema.getType(columnNameInSchema);
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
        final ColumnTypeInfo columnTypeInfo = getColumnTypes().get(columnNameInSchema);
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
     * Returns the index of {@code columnName} in {@code parquetColumnPaths}, or {@link OptionalInt#empty()} if the
     * column is absent. This index is used to find the statistics for a column when pushing down filters.
     */
    private static OptionalInt findColumnIndex(
            @NotNull final String columnName,
            @NotNull final List<String[]> parquetColumnPaths) {
        for (int i = 0; i < parquetColumnPaths.size(); ++i) {
            final String[] path = parquetColumnPaths.get(i);
            if (path.length == 1 && path[0].equals(columnName)) {
                return OptionalInt.of(i);
            }
        }
        return OptionalInt.empty();
    }

    /**
     * Attempts to resolve all columns referenced by {@code filter} against the Parquet schema.
     *
     * @param filter The filter containing the columns to resolve
     * @param renameMap A map of column names to their renamed versions (if applicable)
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
            final String parquetColName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(colNameFromDef);
            final List<String> columnPath = getColumnPath(colNameFromDef, parquetColName);
            if (!isSupportedForPushdown(colNameFromDef, columnPath)) {
                return Optional.empty();
            }

            // Assuming a non-nested column, the first element of the column path is the column name
            final String columnNameFromSchema = columnPath.get(0);
            final OptionalInt columnIndex = findColumnIndex(columnNameFromSchema, pathsFromSchema);
            if (columnIndex.isEmpty()) {
                // Column not found in the schema
                return Optional.empty();
            }
            resolvedColumns.add(new ResolvedColumnInfo(columnPath, columnIndex.getAsInt()));
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

        final List<BlockMetaData> blocks = parquetMetadata.getBlocks();
        iterateRowGroupsAndRowSet(result.maybeMatch(), (rgIdx, rs) -> {
            final Statistics<?> statistics = blocks.get(rgIdx).getColumns().get(columnIndex).getStatistics();
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

    private static final byte ENTRY_UNSEEN = 0;
    private static final byte ENTRY_MATCHES = 1;
    private static final byte ENTRY_FAILS = 2;

    /**
     * Rows read per window of the dictionary scan. A window hands the filter only the entries it is the first to use,
     * so the window does not set the filter's batch size; it sets the working set of the row loops, and this keeps a
     * window's keys and row keys in cache.
     */
    private static final int DICTIONARY_SCAN_CHUNK_SIZE = 4096;

    /**
     * Apply the chunk filter to the row group dictionaries and return the result. Each row's dictionary entry is looked
     * up in a table of entry states. Unless the filter respects a barrier, every entry is evaluated first, in one
     * batch. Otherwise the rows are read in windows, and the entries each window is the first to use are evaluated as
     * one batch, so that the filter sees only the entries the rows in question use.
     */
    @NotNull
    private PushdownResult pushdownFilterDictionary(
            final RowSet selection,
            final WhereFilter filter,
            final RegionedPushdownFilterContext ctx,
            final String[] parquetColumnNames,
            final PushdownResult result) {

        final BasePushdownFilterContext.FilterNullBehavior filterNullBehavior = ctx.filterNullBehavior();
        if (filterNullBehavior == BasePushdownFilterContext.FilterNullBehavior.FAILS_ON_NULLS) {
            // We cannot use dictionary-based filtering, keep all the rows as "maybe" rows. Later when the
            // filter is applied to actual data, it will NPE if there are null values in the table.
            // Future optimization: Use the row-group statistics to determine if there are any nulls in the column
            return result.copy();
        }
        // A null value has no dictionary entry, so whether a null row matches is decided here.
        final boolean nullsMatch = filterNullBehavior == BasePushdownFilterContext.FilterNullBehavior.INCLUDES_NULLS;
        final boolean barrierActive = ExtractRespectedBarriers.stream(filter).findAny().isPresent();

        final RowSetBuilderSequential matchBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential maybeBuilder = RowSetFactory.builderSequential();
        final MutableLong maybeCount = new MutableLong(0);

        final ColumnDefinition<?> columnDefinition = ctx.columnDefinitions().get(0);

        // noinspection unchecked
        final ParquetColumnLocation<Values> columnLocation =
                (ParquetColumnLocation<Values>) getColumnLocation(parquetColumnNames[0]);

        // Get the dictionary chunks for the row groups.
        // noinspection unchecked
        final Chunk<Values>[] dictionaryChunks =
                Arrays.stream(columnLocation.getDictionaryChunkSuppliers(columnDefinition))
                        .map(Supplier::get)
                        .toArray(Chunk[]::new);

        final int maxSize = Arrays.stream(dictionaryChunks)
                .mapToInt(chunk -> chunk == null ? 0 : chunk.size())
                .max()
                .orElse(0);
        if (maxSize == 0) {
            // No dictionaries of non-zero size, nothing to do.
            return result.copy();
        }
        // Every row group's dictionary holds the column's values, so they share a chunk type.
        final ChunkType dictionaryChunkType =
                Arrays.stream(dictionaryChunks).filter(Objects::nonNull).findFirst().orElseThrow().getChunkType();

        // Get the key page stores for the row groups.
        final ColumnChunkPageStore<DictionaryKeys>[] valueStores =
                columnLocation.getDictionaryKeysPageStores(columnDefinition);

        // Allocate a writable chunk sized to the largest dictionary across row groups, and fill it with sequential
        // indices (0..N-1). These values act as candidate dictionary entry IDs, or as positions in a bounded batch.
        try (final WritableLongChunk<OrderedRowKeys> keyCandidates = WritableLongChunk.makeWritableChunk(maxSize);
                final BasePushdownFilterContext.UnifiedChunkFilter chunkFilter = ctx.createChunkFilter(maxSize);
                // A bounded filter's batch: the entries a window is the first to use, at most a whole dictionary.
                final WritableIntChunk<ChunkPositions> newIds =
                        barrierActive ? WritableIntChunk.makeWritableChunk(maxSize) : null;
                final WritableChunk<Values> batchValues =
                        barrierActive ? dictionaryChunkType.makeWritableChunk(maxSize) : null) {
            for (int ii = 0; ii < maxSize; ii++) {
                keyCandidates.set(ii, ii);
            }
            // One state per dictionary entry, sized to the largest dictionary and reset by each row group in turn.
            final byte[] entryStates = new byte[maxSize];
            // Gathers a batch's values from the dictionary by position.
            final PermuteKernel gatherKernel =
                    barrierActive ? PermuteKernel.makePermuteKernel(dictionaryChunkType) : null;

            // Iterate each row group intersecting the current "maybe" rows.
            // NB: "maybe" rows are a subset of "selection" and can not leak information when evaluated by a filter.
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

                final int dictionarySize = dictionaryChunk.size();
                if (barrierActive) {
                    // Entries are evaluated as the rows first use them, below.
                    Arrays.fill(entryStates, 0, dictionarySize, ENTRY_UNSEEN);
                } else {
                    // Evaluate every entry now, in one batch.
                    Arrays.fill(entryStates, 0, dictionarySize, ENTRY_FAILS);
                    try (final WritableLongChunk<OrderedRowKeys> ids = keyCandidates.slice(0, dictionarySize)) {
                        final LongChunk<OrderedRowKeys> matched = chunkFilter.filter(dictionaryChunk, ids);
                        if (matched.size() == 0 && !nullsMatch) {
                            // No entry matches and nulls are excluded, so no row in this row group matches.
                            return;
                        }
                        for (int ii = 0; ii < matched.size(); ++ii) {
                            entryStates[(int) matched.get(ii)] = ENTRY_MATCHES;
                        }
                    }
                }

                // The value store reads offsets within the row group, so the rows are shifted to match it.
                final long subRegionFirstKey = getSubRegionFirstKey(rgIdx);
                // rs is a subset of the "maybe" rows, evaluating these will not leak information outside the selection.
                try (final RowSet rowSet = rs.asRowSet();
                        final RowSet shiftedRowSet = rowSet.shift(-subRegionFirstKey);
                        final RowSequence.Iterator it = shiftedRowSet.getRowSequenceIterator();
                        final ChunkSource.GetContext getContext =
                                valueStore.makeGetContext(DICTIONARY_SCAN_CHUNK_SIZE)) {
                    while (it.hasMore()) {
                        final RowSequence window = it.getNextRowSequenceWithLength(DICTIONARY_SCAN_CHUNK_SIZE);
                        final LongChunk<? extends DictionaryKeys> keys =
                                valueStore.getChunk(getContext, window).asLongChunk();
                        final int windowSize = keys.size();

                        if (barrierActive) {
                            // We need to process newly encountered keys, but should try to chunk them for batch
                            // evaluation for efficiency.
                            int numNew = 0;
                            for (int ii = 0; ii < windowSize; ++ii) {
                                final long key = keys.get(ii);
                                if (key != NULL_LONG && entryStates[(int) key] == ENTRY_UNSEEN) {
                                    entryStates[(int) key] = ENTRY_FAILS;
                                    newIds.set(numNew++, (int) key);
                                }
                            }
                            newIds.setSize(numNew);
                            if (numNew > 0) {
                                // Run the filter on the batch, mark the entries that match.
                                batchValues.setSize(numNew);
                                gatherKernel.permuteInput(dictionaryChunk, newIds, batchValues);
                                try (final WritableLongChunk<OrderedRowKeys> batchPositions =
                                        keyCandidates.slice(0, numNew)) {
                                    final LongChunk<OrderedRowKeys> matched =
                                            chunkFilter.filter(batchValues, batchPositions);
                                    for (int ii = 0; ii < matched.size(); ++ii) {
                                        entryStates[newIds.get((int) matched.get(ii))] = ENTRY_MATCHES;
                                    }
                                }
                            }
                        }

                        // Every entry the window uses is now evaluated; append the matching rows in order.
                        final LongChunk<OrderedRowKeys> rowKeys = window.asRowKeyChunk();
                        for (int ii = 0; ii < windowSize; ++ii) {
                            final long key = keys.get(ii);
                            if (key == NULL_LONG ? nullsMatch : entryStates[(int) key] == ENTRY_MATCHES) {
                                matchBuilder.appendKey(subRegionFirstKey + rowKeys.get(ii));
                            }
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
     * {@code result}. A filter that respects a barrier is shown only the keys of rows in {@code result}'s maybe
     * matches.
     */
    @NotNull
    public PushdownResult pushdownDataIndex(
            final RowSet selection,
            final WhereFilter filter,
            final Map<String, String> renameMap,
            final BasicDataIndex dataIndex,
            final PushdownResult result) {
        final WritableRowSet matching;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final long threshold = (long) (dataIndex.table().size() / QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD);
            if (result.maybeMatch().size() <= threshold) {
                return result.copy();
            }
            final boolean barrierActive = ExtractRespectedBarriers.stream(filter).findAny().isPresent();
            // Extract the fundamental filter, ignoring barriers and serial wrappers.
            final WhereFilter copiedFilter = ExtractFilterWithoutBarriers.of(filter).copy();
            final String rowSetColumnName = dataIndex.rowSetColumnName();
            // Apply the filter to the data index table
            try {
                if (barrierActive && dataIndex instanceof RawTableStandaloneDataIndex) {
                    // The index stores positions within the location, and reading its row sets as row keys converts
                    // every one of them, whether it overlaps the rows in question or not. Filter its raw table in
                    // position space instead: the rows in question are converted to positions once, and the matches
                    // back to row keys once.
                    final Table rawTable = ((RawTableStandaloneDataIndex) dataIndex).rawTable;
                    try (final RowSet locationRowSet = getRowSet();
                            final WritableRowSet positions = locationRowSet.invert(result.maybeMatch());
                            final WritableRowSet matchingPositions = executeFilterDataIndex(
                                    rawTable, rowSetColumnName, renameMap, copiedFilter, true, positions)) {
                        matching = locationRowSet.subSetForPositions(matchingPositions);
                    }
                } else {
                    matching = executeFilterDataIndex(dataIndex.table(), rowSetColumnName, renameMap, copiedFilter,
                            barrierActive, result.maybeMatch());
                }
            } catch (final Exception e) {
                // A cancelled query must stop, not carry on filtering without the index.
                if (CancellationException.isCancellation(e)) {
                    throw e;
                }
                // Filtering the index fails when the read instructions changed the indexed column's type, since the
                // index is read with the types it was written with (DH-19443). Leave every row to the filter itself.
                log.warn().append("Skipping data index pushdown for filter ").append(String.valueOf(filter))
                        .append(": ").append(String.valueOf(e)).endl();
                return result.copy();
            }
        }
        // Retain only the maybe rows and add the previously found matches.
        try (matching; final WritableRowSet empty = RowSetFactory.empty()) {
            matching.insert(result.match());
            return PushdownResult.of(selection, matching, empty);
        }
    }

    /**
     * Apply {@code filter} to {@code indexTable} and return the union of the matching index rows' row sets, each
     * intersected with {@code rows}. Behind a barrier, the filter is applied only to the index rows whose row sets
     * overlap {@code rows}. The index's row sets and {@code rows} must be in the same space, row keys or positions.
     *
     * @return The matches, in the space of {@code rows}; ownership passes to the caller
     */
    private static WritableRowSet executeFilterDataIndex(
            final Table indexTable,
            final String rowSetColumnName,
            final Map<String, String> renameMap,
            final WhereFilter filter,
            final boolean barrierActive,
            final RowSet rows) {
        Table toFilter = indexTable;
        if (!renameMap.isEmpty()) {
            final Collection<Pair> renamePairs = renameMap.entrySet().stream()
                    .map(entry -> io.deephaven.api.Pair.of(ColumnName.of(entry.getValue()),
                            ColumnName.of(entry.getKey())))
                    .collect(Collectors.toList());
            toFilter = toFilter.renameColumns(renamePairs);
        }
        if (barrierActive) {
            // Keep only the index rows whose row sets overlap the rows in question, in one ordered pass. A where would
            // make the same overlaps call per row, as there is no chunk kernel for it, only through a computed overlap
            // column, a filter and an intermediate table; and the locations are already pushed down in parallel, so
            // its parallelism would buy nothing.
            final RowSetBuilderSequential overlapping = RowSetFactory.builderSequential();
            try (final RowSet.Iterator indexRows = toFilter.getRowSet().iterator();
                    final CloseableIterator<RowSet> rowSets =
                            ColumnVectors.ofObject(toFilter, rowSetColumnName, RowSet.class).iterator()) {
                while (rowSets.hasNext()) {
                    final long indexRow = indexRows.nextLong();
                    if (rowSets.next().overlaps(rows)) {
                        overlapping.appendKey(indexRow);
                    }
                }
            }
            toFilter = toFilter.getSubTable(overlapping.build().toTracking());
        }
        final Table filteredTable = toFilter.where(filter);

        // One row set per index row that passes the filter. The batcher owns what it gathers, and building after the
        // iterator is closed leaves nothing that can throw between building and returning the result.
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(filteredTable.size())) {
            try (final CloseableIterator<RowSet> it =
                    ColumnVectors.ofObject(filteredTable, rowSetColumnName, RowSet.class).iterator()) {
                it.forEachRemaining(rowSet -> batcher.add(rowSet.intersect(rows)));
            }
            return batcher.build();
        }
    }

    // endregion Pushdown Filtering
}

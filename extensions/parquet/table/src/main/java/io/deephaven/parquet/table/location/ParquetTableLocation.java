//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.StandaloneDataIndex;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPageStore;
import io.deephaven.engine.table.impl.util.JobScheduler;
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
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.util.type.NumericTypeUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;
import static io.deephaven.parquet.table.ParquetTableWriter.*;
import static io.deephaven.parquet.table.ParquetTableWriter.GROUPING_END_POS_COLUMN_NAME;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;

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
            final long subRegionFirstKey = (long) rgi << regionParameters.regionMaskNumBits;
            final long subRegionLastKey = subRegionFirstKey + subRegionSize - 1;
            sequentialBuilder.appendRange(subRegionFirstKey, subRegionLastKey);
        }
        return sequentialBuilder.build();
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
        return StandaloneDataIndex.from(table, columns, INDEX_ROW_SET_COLUMN_NAME);
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
     * Read a Data Index table from disk.
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
                            (final long rowKey, final ColumnSource<?>[] sources) -> RowSetFactory
                                    .fromRange(sources[0].getLong(rowKey), sources[1].getLong(rowKey) - 1))));
        } else {
            throw new TableDataException(String.format(
                    "Index table %s for table %s was not in the expected format. Expected columns [%s] but encountered [%s]",
                    indexFileMetaData.fileURI, parentFileURI,
                    String.join(", ",
                            GROUPING_KEY_COLUMN_NAME, GROUPING_BEGIN_POS_COLUMN_NAME, GROUPING_END_POS_COLUMN_NAME),
                    indexTable.getDefinition().getColumnNamesAsString()));
        }
    }

    @Override
    public long estimatePushdownFilterCost(
            final WhereFilter filter,
            final Map<String, String> renameMap,
            final RowSet selection,
            final RowSet fullSet,
            final boolean usePrev,
            final PushdownFilterContext context) {
        if (selection.isEmpty()) {
            // If the selection is empty, we can skip all pushdown filtering.
            log.warn().append("Estimate pushdown filter cost called with empty selection for table ")
                    .append(getTableKey()).endl();
            return Long.MAX_VALUE;
        }
        final long executedFilterCost = context.executedFilterCost();

        // Some range filters host a condition filter as the internal filter, and we can't push that down.
        final boolean isRangeFilter =
                filter instanceof RangeFilter && ((RangeFilter) filter).getRealFilter() instanceof AbstractRangeFilter;
        final boolean isMatchFilter = filter instanceof MatchFilter;

        final ResolvedColumnsInfo resolvedColumnsInfo;
        try {
            resolvedColumnsInfo = resolveColumns(filter, renameMap);
        } catch (final RuntimeException e) {
            // Failed to find the columns in the Parquet schema, so no benefit to pushing down.
            return Long.MAX_VALUE;
        }

        if (shouldExecute(QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA,
                PushdownResult.METADATA_STATS_COST, executedFilterCost)
                && (isRangeFilter || isMatchFilter)) {
            return PushdownResult.METADATA_STATS_COST;
        }

        final String[] parquetColumnNames = resolvedColumnsInfo.columnNames;

        // Do we have a data indexes for the column(s)?
        if (shouldExecute(QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX,
                PushdownResult.IN_MEMORY_DATA_INDEX_COST, executedFilterCost)
                && hasCachedDataIndex(parquetColumnNames)) {
            return PushdownResult.IN_MEMORY_DATA_INDEX_COST;
        }
        if (shouldExecute(QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX,
                PushdownResult.DEFERRED_DATA_INDEX_COST, executedFilterCost)
                && hasDataIndex(parquetColumnNames)) {
            return PushdownResult.DEFERRED_DATA_INDEX_COST;
        }

        // TODO(DH-19666): Add support for bloom filters, sortedness, etc.
        return Long.MAX_VALUE; // No benefit to pushing down.
    }

    /**
     * A helper class to hold the resolved column names and their corresponding field IDs.
     */
    private static class ResolvedColumnsInfo {
        final String[] columnNames;
        final List<Integer> fieldIds;

        ResolvedColumnsInfo(
                @NotNull final String[] columnNames,
                @NotNull final List<Integer> fieldIds) {
            this.columnNames = columnNames;
            this.fieldIds = fieldIds;
            Require.requirement(columnNames.length == fieldIds.size(),
                    "columnNames and fieldIds must have the same length");
        }
    }

    private List<List<String>> getParquetColumnPaths(
            final Collection<String> colNamesFromFilter,
            final Map<String, String> renameMap) {
        return colNamesFromFilter.stream()
                .map(colNameFromFilter -> {
                    final String colNameFromDef = renameMap.getOrDefault(colNameFromFilter, colNameFromFilter);
                    final String parquetColName =
                            readInstructions.getParquetColumnNameFromColumnNameOrDefault(colNameFromDef);
                    return getColumnPath(colNameFromDef, parquetColName);
                })
                .collect(Collectors.toList());
    }

    private List<Integer> getParquetIndices(final Collection<List<String>> parquetColumnPaths) {
        // Get the list of paths from the schema, and then find the indices of the column's path in that list.
        final Map<List<String>, Integer> pathToFieldId = ParquetSchemaUtil.getPathToFieldId(parquetSchema);
        final List<Integer> indices = new ArrayList<>(parquetColumnPaths.size());
        for (final List<String> columnPath : parquetColumnPaths) {
            if (columnPath.isEmpty()) {
                throw new IllegalArgumentException(
                        "Parquet paths must contain at least one element, found " + columnPath);
            }
            final Integer idx = pathToFieldId.get(columnPath);
            if (idx == null) {
                throw new IllegalArgumentException(String.format(
                        "Parquet schema does not contain column '%s' for table %s",
                        columnPath, getTableKey()));
            }
            indices.add(idx);
        }
        return indices;
    }

    /**
     * Verify that the provided Parquet column paths do not contain nested types, like lists or structs.
     *
     * @throws IllegalStateException if any of the paths contain nested types
     */
    private static void ensureFlatColumns(
            final List<List<String>> parquetColumnPaths,
            final MessageType schema) {
        for (final List<String> path : parquetColumnPaths) {
            if (schema.getMaxRepetitionLevel(path.toArray(new String[0])) > 0) {
                throw new IllegalArgumentException("Pushdown filtering is not supported for nested types, found " +
                        "nested type at path: " + String.join(".", path));
            }
        }
    }

    /**
     * Resolve the columns from the filter against the Parquet schema.
     *
     * @param filter The filter containing the columns to resolve
     * @param renameMap A map of column names to their renamed versions
     * @return A {@link ResolvedColumnsInfo} containing the resolved column names and their field IDs
     * @throws IllegalArgumentException if any of the columns in the filter cannot be resolved using the Parquet schema
     */
    private ResolvedColumnsInfo resolveColumns(
            @NotNull final WhereFilter filter,
            @NotNull final Map<String, String> renameMap) {

        final List<List<String>> parquetColPaths = getParquetColumnPaths(filter.getColumns(), renameMap);

        ensureFlatColumns(parquetColPaths, parquetSchema);

        final List<Integer> fieldIds = getParquetIndices(parquetColPaths);
        final String[] names = fieldIds.stream()
                .map(parquetSchema::getFieldName)
                .toArray(String[]::new);

        return new ResolvedColumnsInfo(names, fieldIds);
    }

    @Override
    public void pushdownFilter(
            final WhereFilter filter,
            final Map<String, String> renameMap,
            final RowSet selection,
            final RowSet fullSet,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        if (selection.isEmpty()) {
            log.warn().append("Pushdown filter called with empty selection for table ").append(getTableKey()).endl();
            onComplete.accept(PushdownResult.of(RowSetFactory.empty(), RowSetFactory.empty()));
            return;
        }

        // Initialize the pushdown result with the selection rowset as "maybe" rows
        PushdownResult result = PushdownResult.of(RowSetFactory.empty(), selection.copy());

        final long executedFilterCost = context.executedFilterCost();
        final ResolvedColumnsInfo resolvedColumnsInfo;
        try {
            resolvedColumnsInfo = resolveColumns(filter, renameMap);
        } catch (final RuntimeException e) {
            // Failed to find the columns in the Parquet schema. So we return all rows as "maybe" rows.
            onComplete.accept(result);
            return;
        }

        final List<Integer> fieldIds = resolvedColumnsInfo.fieldIds;
        final String[] parquetColumnNames = resolvedColumnsInfo.columnNames;

        // Should we look at the metadata?
        if (shouldExecute(QueryTable.DISABLE_WHERE_PUSHDOWN_PARQUET_ROW_GROUP_METADATA,
                PushdownResult.METADATA_STATS_COST, executedFilterCost, costCeiling)) {
            // Some range filters host a condition filter as the internal filter and we can't push that down.
            if (filter instanceof RangeFilter
                    && ((RangeFilter) filter).getRealFilter() instanceof AbstractRangeFilter) {
                try (final PushdownResult ignored = result) {
                    result = pushdownRangeFilter((AbstractRangeFilter) ((RangeFilter) filter).getRealFilter(),
                            fieldIds, result);
                }
            } else if (filter instanceof MatchFilter) {
                try (final PushdownResult ignored = result) {
                    result = pushdownMatchFilter((MatchFilter) filter, fieldIds, result);
                }
            }
        }
        if (result.maybeMatch().isEmpty()) {
            // No maybe rows remaining, so no reason to continue filtering.
            onComplete.accept(result);
            return;
        }

        // If not prohibited by the cost ceiling, continue to refine the pushdown results.

        if (shouldExecute(QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX,
                PushdownResult.IN_MEMORY_DATA_INDEX_COST, executedFilterCost, costCeiling)) {
            final BasicDataIndex dataIndex =
                    hasCachedDataIndex(parquetColumnNames) ? getDataIndex(parquetColumnNames) : null;
            if (dataIndex != null) {
                // No maybe rows remaining, so no reason to continue filtering.
                try (final PushdownResult ignored = result) {
                    onComplete.accept(pushdownDataIndex(filter, renameMap, dataIndex, result));
                    return;
                }
            }
        }

        if (shouldExecute(QueryTable.DISABLE_WHERE_PUSHDOWN_DATA_INDEX,
                PushdownResult.DEFERRED_DATA_INDEX_COST, executedFilterCost, costCeiling)) {
            // If we have a data index, apply the filter to the data index table and retain the incoming maybe rows.
            final BasicDataIndex dataIndex = hasDataIndex(parquetColumnNames) ? getDataIndex(parquetColumnNames) : null;
            if (dataIndex != null) {
                // No maybe rows remaining, so no reason to continue filtering.
                try (final PushdownResult ignored = result) {
                    onComplete.accept(pushdownDataIndex(filter, renameMap, dataIndex, result));
                    return;
                }
            }
        }

        onComplete.accept(result);
    }

    /**
     * Helper methods to determine if we should execute this push-down technique.
     */
    private boolean shouldExecute(final boolean disable, final long filterCost, final long executedFilterCost) {
        return !disable && executedFilterCost < filterCost;
    }

    private boolean shouldExecute(final boolean disable, final long filterCost, final long executedFilterCost,
            final long costCeiling) {
        return shouldExecute(disable, filterCost, executedFilterCost) && filterCost <= costCeiling;
    }

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
                final long subRegionFirstKey = (long) rgIdx << regionParameters.regionMaskNumBits;
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
     * Get the min and max values from the statistics. Currently can convert basic numerics, string and BigDecimal /
     * BigInteger values.
     *
     * @param statistics The statistics to analyze
     * @return The min and max values from the statistics or null if cannot be converted.
     */
    private Pair<Object, Object> getMinMax(final Statistics<?> statistics) {
        if (statistics == null || statistics.isEmpty()) {
            return null;
        }
        // Min/Max are guaranteed to be the same type, only testing min.
        final Class<?> clazz = statistics.genericGetMin().getClass();

        // Numeric values need no conversion
        if (NumericTypeUtils.isIntegralOrChar(clazz) || NumericTypeUtils.isFloat(clazz)) {
            return new Pair<>(statistics.genericGetMin(), statistics.genericGetMax());
        }

        if (statistics.type().getLogicalTypeAnnotation() == stringType()) {
            return new Pair<>(statistics.minAsString(), statistics.maxAsString());
        }

        if (statistics.type()
                .getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            final int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) statistics.type()
                    .getLogicalTypeAnnotation()).getScale();
            if (scale == 0) {
                // Return the min and max values as BigInteger
                return new Pair<>(new BigInteger(statistics.getMinBytes()), new BigInteger(statistics.getMaxBytes()));
            } else {
                // We need to convert the min and max values to BigDecimal
                return new Pair<>(new BigDecimal(new BigInteger(statistics.getMinBytes()), scale),
                        new BigDecimal(new BigInteger(statistics.getMaxBytes()), scale));
            }
        }
        return null;
    }

    /**
     * Apply the range filter to the row groups and return the result.
     */
    @NotNull
    private PushdownResult pushdownRangeFilter(
            final AbstractRangeFilter rf,
            final List<Integer> fieldIds,
            final PushdownResult result) {
        final RowSetBuilderSequential maybeBuilder = RowSetFactory.builderSequential();
        final MutableLong maybeCount = new MutableLong(0);

        // Only one column in a RangeFilter
        final Integer fieldId = fieldIds.get(0);
        final List<BlockMetaData> blocks = parquetMetadata.getBlocks();
        iterateRowGroupsAndRowSet(result.maybeMatch(), (rgIdx, rs) -> {
            final Pair<Object, Object> p = getMinMax(
                    blocks.get(rgIdx).getColumns().get(fieldId).getStatistics());

            if (p == null || rf.overlaps(p.first, p.second)) {
                maybeBuilder.appendRowSequence(rs);
                maybeCount.add(rs.size());
            }
        });
        return PushdownResult.of(result.match().copy(),
                maybeCount.get() == result.maybeMatch().size() ? result.maybeMatch().copy() : maybeBuilder.build());
    }

    /**
     * Apply the match filter to the row groups and return the result.
     */
    @NotNull
    private PushdownResult pushdownMatchFilter(
            final MatchFilter mf,
            final List<Integer> parquetIndices,
            final PushdownResult result) {
        final RowSetBuilderSequential maybeBuilder = RowSetFactory.builderSequential();
        final MutableLong maybeCount = new MutableLong(0);

        // Only one column in a RangeFilter
        final Integer parquetIndex = parquetIndices.get(0);

        iterateRowGroupsAndRowSet(result.maybeMatch(), (rgIdx, rs) -> {
            final Pair<Object, Object> p = getMinMax(
                    parquetMetadata.getBlocks().get(rgIdx).getColumns().get(parquetIndex).getStatistics());

            if (p == null || mf.overlaps(p.first, p.second)) {
                maybeBuilder.appendRowSequence(rs);
                maybeCount.add(rs.size());
            }
        });
        return PushdownResult.of(result.match().copy(),
                maybeCount.get() == result.maybeMatch().size() ? result.maybeMatch().copy() : maybeBuilder.build());
    }

    /**
     * Apply the filter to the data index table and return the result.
     */
    @NotNull
    private PushdownResult pushdownDataIndex(
            final WhereFilter filter,
            final Map<String, String> renameMap,
            final BasicDataIndex dataIndex,
            final PushdownResult result) {
        final RowSetBuilderRandom matchingBuilder = RowSetFactory.builderRandom();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final WhereFilter copiedFilter = filter.copy();
            copiedFilter.init(dataIndex.table().getDefinition());

            // TODO: When https://deephaven.atlassian.net/browse/DH-19443 is implemented, we should be able
            // to use the filter directly on the index table.
            final Collection<io.deephaven.api.Pair> renamePairs = renameMap.entrySet().stream()
                    .map(entry -> io.deephaven.api.Pair.of(ColumnName.of(entry.getValue()),
                            ColumnName.of(entry.getKey())))
                    .collect(Collectors.toList());
            final Table renamedIndexTable = dataIndex.table().renameColumns(renamePairs);

            // Apply the filter to the data index table
            try {
                final Table filteredTable = renamedIndexTable.where(copiedFilter);

                try (final CloseableIterator<RowSet> it =
                        ColumnVectors.ofObject(filteredTable, dataIndex.rowSetColumnName(), RowSet.class).iterator()) {
                    it.forEachRemaining(matchingBuilder::addRowSet);
                }
            } catch (final Exception e) {
                // Exception occurs here if we have a data type mismatch between the index and the filter.
                // Just swallow the exception and declare all the rows as maybe matches.
                return PushdownResult.of(RowSetFactory.empty(), result.maybeMatch().copy());
            }
        }
        // Retain only the maybe rows and add the previously found matches.
        final WritableRowSet matching = matchingBuilder.build();
        matching.retain(result.maybeMatch());
        matching.insert(result.match());
        return PushdownResult.of(matching, RowSetFactory.empty());
    }
}

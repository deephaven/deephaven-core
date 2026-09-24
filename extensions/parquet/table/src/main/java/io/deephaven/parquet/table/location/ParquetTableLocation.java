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
import io.deephaven.parquet.table.SortedColumnsExclusion;
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
    /**
     * With a {@link #resolver}, the Deephaven column reading each top-level parquet column, from the table definition;
     * see {@link #resolvedParquetColumnNameToColumnName()}.
     */
    private volatile Map<String, String> resolvedParquetColumnNameToColumnName;

    private RegionedPageStore.Parameters regionParameters;
    private Map<String, String[]> parquetColumnNameToPath;

    /**
     * The Deephaven metadata in the file footer. It names columns by their parquet names -- the names in the file's
     * schema -- which differ from the Deephaven names whenever the read renames or resolves columns; translate with
     * {@link #toColumnName(String)} and {@link #toParquetColumnName(String)} before crossing that boundary.
     */
    private TableInfo tableInfo;
    /** Keyed by parquet column name. */
    private Map<String, GroupingColumnInfo> groupingColumns;
    /** Keyed by parquet column name. */
    private Map<String, ColumnTypeInfo> columnTypes;
    /** In Deephaven column names; see {@link #getSortedColumns()}. */
    private volatile List<SortColumn> sortingColumns;

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
                final List<RowGroup> fileRowGroups = parquetFileReader.fileMetaData.getRow_groups();
                rowGroupIndices = IntStream.of(tableLocationKey.getRowGroupIndices())
                        .boxed()
                        .sorted(Comparator.comparingInt(rgi -> fileRowGroups.get(rgi).getOrdinal()))
                        .mapToInt(Integer::intValue)
                        .toArray();
            }

            resolver = readInstructions.getColumnResolverFactory()
                    .map(factory -> factory.of(getTableKey(), tableLocationKey))
                    .orElse(null);
            final int rowGroupCount = rowGroupIndices.length;
            final RowGroup[] rowGroups = IntStream.of(rowGroupIndices)
                    .mapToObj(rgi -> parquetFileReader.fileMetaData.getRow_groups().get(rgi))
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
            return sortingColumns = SortedColumnsExclusion.apply(
                    readInstructions.getSortedColumnsExclusions(),
                    toColumnSortColumns(SortColumnInfo.sortColumns(tableInfo.sortingColumns())),
                    readInstructions.getTableDefinition().orElse(null));
        }
    }

    /**
     * With a {@link #resolver}, maps each top-level parquet column to the Deephaven column reading it, from the table
     * definition. A parquet column read by more than one Deephaven column is absent, since no single column can be
     * identified as reading it. Built on first use, since resolving every column of the definition is work that reading
     * only some of them does not need. Requires initialization.
     */
    @NotNull
    private Map<String, String> resolvedParquetColumnNameToColumnName() {
        Map<String, String> local;
        if ((local = resolvedParquetColumnNameToColumnName) != null) {
            return local;
        }
        synchronized (this) {
            if ((local = resolvedParquetColumnNameToColumnName) != null) {
                return local;
            }
            final Map<String, String> parquetToDeephaven = new HashMap<>();
            final Set<String> readByMany = new HashSet<>();
            readInstructions.getTableDefinition().ifPresent(definition -> {
                for (final ColumnDefinition<?> column : definition.getColumns()) {
                    final List<String> columnPath;
                    try {
                        columnPath = resolver.of(column.getName()).orElse(List.of());
                    } catch (TableDataException e) {
                        // The column cannot be resolved in this file, so it reads none of its columns; reading it
                        // reports the error
                        continue;
                    }
                    if (columnPath.size() == 1
                            && parquetToDeephaven.putIfAbsent(columnPath.get(0), column.getName()) != null) {
                        readByMany.add(columnPath.get(0));
                    }
                }
            });
            parquetToDeephaven.keySet().removeAll(readByMany);
            return resolvedParquetColumnNameToColumnName = Collections.unmodifiableMap(parquetToDeephaven);
        }
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
        return getColumnPathInternal(columnName, parquetColumnNameOrDefault);
    }

    /**
     * Returns the name of the top-level parquet column that the Deephaven column {@code columnName} reads, or
     * {@code null} if it does not read exactly one top-level column of this file. Requires initialization.
     */
    @Nullable
    private String toParquetColumnName(@NotNull final String columnName) {
        final List<String> columnPath = getColumnPathInternal(columnName,
                readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName));
        if (columnPath.size() != 1 || !parquetSchema.containsField(columnPath.get(0))) {
            return null;
        }
        final String parquetColumnName = columnPath.get(0);
        // Without a resolver the default mapping is the identity, which maps a name that is not a column -- one whose
        // parquet column the instructions give another name -- to that parquet column; a resolver is authoritative.
        return resolver != null || columnName.equals(
                readInstructions.getColumnNameFromParquetColumnNameOrDefault(parquetColumnName))
                        ? parquetColumnName
                        : null;
    }

    /**
     * Returns the name of the Deephaven column that reads the top-level parquet column {@code parquetColumnName}, or
     * {@code null} if no Deephaven column can be identified as reading it. Requires initialization.
     */
    @Nullable
    private String toColumnName(@NotNull final String parquetColumnName) {
        final String columnName = resolver == null
                ? readInstructions.getColumnNameFromParquetColumnNameOrDefault(parquetColumnName)
                : resolvedParquetColumnNameToColumnName().get(parquetColumnName);
        // Accept the name only if it reads this very column: the default mapping is the identity, and that names a
        // column the instructions may have mapped to a different parquet column.
        return columnName != null && parquetColumnName.equals(toParquetColumnName(columnName))
                ? columnName
                : null;
    }

    /**
     * Like {@link #toParquetColumnName(String)}, for a data index key column, which may also be a column that is not
     * read from this file, such as a partitioning column. The metadata records such a column under the parquet name the
     * instructions map it to. Requires initialization.
     */
    @Nullable
    private String toIndexParquetColumnName(@NotNull final String columnName) {
        final String parquetColumnName = toParquetColumnName(columnName);
        if (parquetColumnName != null) {
            return parquetColumnName;
        }
        final String mappedName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName);
        // A name that is in the file belongs to the column read from it, which is not this one
        return !parquetSchema.containsField(mappedName)
                && columnName.equals(readInstructions.getColumnNameFromParquetColumnNameOrDefault(mappedName))
                        ? mappedName
                        : null;
    }

    /**
     * Like {@link #toColumnName(String)}, for a data index key column, which may also be a column that is not read from
     * this file, such as a partitioning column. Requires initialization.
     */
    @Nullable
    private String toIndexColumnName(@NotNull final String parquetColumnName) {
        if (parquetSchema.containsField(parquetColumnName)) {
            return toColumnName(parquetColumnName);
        }
        final String columnName = readInstructions.getColumnNameFromParquetColumnNameOrDefault(parquetColumnName);
        return parquetColumnName.equals(toIndexParquetColumnName(columnName)) ? columnName : null;
    }

    /**
     * Translates the Deephaven names of data index key columns to the parquet names the metadata records them under, or
     * returns {@code null} if any of them cannot be. Requires initialization.
     */
    @Nullable
    private String[] toIndexParquetColumnNames(@NotNull final String[] columnNames) {
        final String[] parquetColumnNames = new String[columnNames.length];
        for (int ci = 0; ci < columnNames.length; ++ci) {
            if ((parquetColumnNames[ci] = toIndexParquetColumnName(columnNames[ci])) == null) {
                return null;
            }
        }
        return parquetColumnNames;
    }

    /**
     * Translates the parquet names of data index key columns, as the metadata records them, to Deephaven column names,
     * or returns {@code null} if any of them cannot be. Requires initialization.
     */
    @Nullable
    private String[] toIndexColumnNames(@NotNull final Collection<String> parquetColumnNames) {
        final String[] columnNames = new String[parquetColumnNames.size()];
        int ci = 0;
        for (final String parquetColumnName : parquetColumnNames) {
            if ((columnNames[ci++] = toIndexColumnName(parquetColumnName)) == null) {
                return null;
            }
        }
        return columnNames;
    }

    /**
     * Translates the file's sort columns, which name parquet columns, to Deephaven column names. A sort is only
     * meaningful as a prefix -- each column is sorted within runs of the columns before it -- so translation stops at
     * the first column that cannot be identified. Requires initialization.
     * <p>
     * The sortedness itself is trusted rather than verified, as
     * {@link io.deephaven.engine.table.impl.SortedColumnsAttribute} documents. What this guards is the identity of the
     * column. Files written before the writer recorded parquet column names recorded the Deephaven name at write time
     * instead; the two agree unless the sorted column was renamed on write, in which case the name is normally absent
     * from the file and the sort is dropped. The one case that cannot be detected is a write that swapped two column
     * names, so that the recorded name belongs to the other column.
     */
    @NotNull
    private List<SortColumn> toColumnSortColumns(@NotNull final List<SortColumn> fileSortColumns) {
        final List<SortColumn> sortColumns = new ArrayList<>(fileSortColumns.size());
        for (final SortColumn fileSortColumn : fileSortColumns) {
            final String columnName = toColumnName(fileSortColumn.column().name());
            if (columnName == null) {
                break;
            }
            sortColumns.add(fileSortColumn.order() == SortColumn.Order.ASCENDING
                    ? SortColumn.asc(ColumnName.of(columnName))
                    : SortColumn.desc(ColumnName.of(columnName)));
        }
        return Collections.unmodifiableList(sortColumns);
    }

    private List<String> getColumnPathInternal(@NotNull String columnName, String parquetColumnNameOrDefault) {
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
        // The metadata names parquet columns, and callers expect Deephaven column names. An index whose columns cannot
        // all be identified is omitted.
        // Add the data indexes to the list
        tableInfo.dataIndexes().stream()
                .map(di -> toIndexColumnNames(di.columns()))
                .filter(Objects::nonNull)
                .forEach(dataIndexColumns::add);
        // Add grouping columns to the list
        groupingColumns.keySet().stream()
                .map(colName -> toIndexColumnNames(List.of(colName)))
                .filter(Objects::nonNull)
                .forEach(dataIndexColumns::add);
        return dataIndexColumns;
    }

    @Override
    public boolean hasDataIndex(@NotNull final String... columns) {
        initialize();
        if (tableInfo.dataIndexes().isEmpty() && groupingColumns.isEmpty()) {
            return false;
        }
        // The metadata names parquet columns
        final String[] parquetColumns = toIndexParquetColumnNames(columns);
        if (parquetColumns == null) {
            return false;
        }
        // Check if the column name matches any of the grouping columns
        if (parquetColumns.length == 1 && groupingColumns.containsKey(parquetColumns[0])) {
            // Validate the index file exists (without loading and parsing it)
            final IndexFileMetadata metadata = getIndexFileMetadata(getParquetKey().getURI(), parquetColumns);
            return metadata != null && parquetFileExists(metadata.fileURI);
        }
        // Check if the column names match any of the data indexes
        for (final DataIndexInfo dataIndex : tableInfo.dataIndexes()) {
            if (dataIndex.matchesColumns(parquetColumns)) {
                // Validate the index file exists (without loading and parsing it)
                final IndexFileMetadata metadata = getIndexFileMetadata(getParquetKey().getURI(), parquetColumns);
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
        final String[] parquetColumns = toIndexParquetColumnNames(columns);
        final IndexFileMetadata indexFileMetaData = parquetColumns == null
                ? null
                : getIndexFileMetadata(getParquetKey().getURI(), parquetColumns);
        if (indexFileMetaData == null) {
            throw new TableDataException(
                    String.format(
                            "No index metadata for table %s with index key columns %s was present in TableInfo",
                            getParquetKey().getURI(), Arrays.toString(columns)));
        }
        // Create a new index from the parquet table
        final Table table =
                readDataIndexTable(getParquetKey().getURI(), indexFileMetaData, readInstructions, parquetColumns,
                        columns);
        if (table == null) {
            return null;
        }
        final RowSet locationRowSet = getRowSet();
        final Table adjustedTable = locationRowSet.isFlat() ? table
                : table.updateView(List.of(new FunctionalColumn<>(
                        INDEX_ROW_SET_COLUMN_NAME, RowSet.class,
                        INDEX_ROW_SET_COLUMN_NAME, RowSet.class,
                        (final RowSet indexRowSet) -> locationRowSet.subSetForPositions(indexRowSet))));
        return StandaloneDataIndex.from(adjustedTable, columns, INDEX_ROW_SET_COLUMN_NAME);
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
     * @param parquetInstructions The instructions for reading the table. The index table is written with the parquet
     *        column names of the table it indexes, so reading it with the same renames names its key columns with the
     *        Deephaven column names. A column resolver describes the indexed table's files, not the index table's, so
     *        it is not used; the key columns are renamed instead.
     * @param parquetKeyColumnNames The parquet names of the key columns
     * @param keyColumnNames The Deephaven names of the key columns
     *
     * @return The data index table for the specified key columns or {@code null} if none was found
     */
    @Nullable
    private static Table readDataIndexTable(
            @NotNull final URI parentFileURI,
            @NotNull final ParquetTableLocation.IndexFileMetadata indexFileMetaData,
            @NotNull final ParquetInstructions parquetInstructions,
            @NotNull final String[] parquetKeyColumnNames,
            @NotNull final String[] keyColumnNames) {
        final ParquetInstructions indexReadInstructions = parquetInstructions.getColumnResolverFactory().isEmpty()
                ? parquetInstructions.withTableDefinitionAndLayout(null,
                        ParquetInstructions.ParquetFileLayout.SINGLE_FILE)
                : new ParquetInstructions.Builder(parquetInstructions)
                        .setColumnResolverFactory(null)
                        .setTableDefinition(null)
                        .setFileLayout(ParquetInstructions.ParquetFileLayout.SINGLE_FILE)
                        .build();
        final Table indexTable = ParquetTools.readTable(indexFileMetaData.fileURI.toString(), indexReadInstructions);
        if (indexFileMetaData.dataIndexInfo != null) {
            // Name any key column the read did not rename with its Deephaven name
            final List<Pair> renames = new ArrayList<>();
            for (int ki = 0; ki < keyColumnNames.length; ++ki) {
                if (!indexTable.hasColumns(keyColumnNames[ki]) && indexTable.hasColumns(parquetKeyColumnNames[ki])) {
                    renames.add(Pair.of(ColumnName.of(parquetKeyColumnNames[ki]), ColumnName.of(keyColumnNames[ki])));
                }
            }
            return renames.isEmpty() ? indexTable : indexTable.renameColumns(renames);
        }
        Assert.neqNull(indexFileMetaData.groupingColumnInfo, "indexFileMetaData.groupingColumnInfo");
        if (indexTable.hasColumns(
                GROUPING_KEY_COLUMN_NAME, GROUPING_BEGIN_POS_COLUMN_NAME, GROUPING_END_POS_COLUMN_NAME)) {
            // Legacy grouping tables are written with a key, start position, and end position. We must convert the
            // ranges to RowSets.
            return indexTable.view(List.of(
                    new SourceColumn(GROUPING_KEY_COLUMN_NAME, keyColumnNames[0]),
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
     * Checks whether any row group of the column might have a dictionary the dictionary action can use. This is judged
     * from metadata alone, so that estimating does not read dictionaries, and it considers every row group, since a
     * writer can fall back from dictionary encoding in some row groups but not others. The action itself reads the
     * dictionaries, and keeps a row group's rows as "maybe" when it has none.
     *
     * @param columnName The Deephaven name of the column
     * @param columnDefinition The definition of the column (required to access dictionary chunk suppliers)
     * @return {@code false} if no row group has a usable dictionary, {@code true} if one might
     */
    private boolean mayHaveDictionaryPages(final String columnName, final ColumnDefinition<?> columnDefinition) {
        return ((ParquetColumnLocation<?>) getColumnLocation(columnName)).mayHaveDictionaryPages(columnDefinition);
    }

    public static class EstimateContext implements RegionedPushdownAction.EstimateContext {
        enum ResolveState {
            RESOLVED, FAILED
        }

        private final ResolveState resolveState;
        /** The Deephaven names of the filter's columns. */
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
            isApplicable = mayHaveDictionaryPages(estimateCtx.columnNames[0], filterCtx.columnDefinitions().get(0));
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
        /** The Deephaven names of the filter's columns. */
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
            if (!mayHaveDictionaryPages(actionCtx.columnNames[0], filterCtx.columnDefinitions().get(0))) {
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
     * A helper class to hold a filter column's Deephaven name and the index of the parquet column it reads.
     */
    private static class ResolvedColumnInfo {
        /**
         * The Deephaven column name, which is what {@link #getColumnLocation}, the data index methods and the other
         * {@link TableLocation} APIs expect.
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
                try (final RowSet shiftedRowSet = rs.asRowSet().shift(-subRegionFirstKey);
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
     * Apply the filter to the data index table and return the result.
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
                // TODO: Exception occurs here if we have a data type mismatch between the index and the filter.
                // When https://deephaven.atlassian.net/browse/DH-19443 is implemented, we should be able
                // to remove the catch block and let any exception propagate. For now, just swallow the exception
                // and return a copy of the original input, skipping pushdown filtering.
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

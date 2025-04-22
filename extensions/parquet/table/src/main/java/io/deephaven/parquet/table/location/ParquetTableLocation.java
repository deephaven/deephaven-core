//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.dataindex.StandaloneDataIndex;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocationState;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.select.MultiSourceFunctionalColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPageStore;
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
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.IntStream;

import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;
import static io.deephaven.parquet.table.ParquetTableWriter.*;
import static io.deephaven.parquet.table.ParquetTableWriter.GROUPING_END_POS_COLUMN_NAME;

public class ParquetTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

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
    private int[] rowGroupIndices;
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
            final ParquetMetadata parquetMetadata;
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

            parquetColumnNameToPath = new HashMap<>();
            for (String[] path : ParquetSchemaUtil.paths(parquetFileReader.getSchema())) {
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
        // of this codee, as it's not expected that such tables exist in the wild.
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
}

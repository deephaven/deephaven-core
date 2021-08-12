package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.impl.AbstractTableLocation;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetSchemaReader;
import io.deephaven.db.v2.parquet.metadata.ColumnTypeInfo;
import io.deephaven.db.v2.parquet.metadata.GroupingColumnInfo;
import io.deephaven.db.v2.parquet.metadata.TableInfo;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.regioned.RegionedColumnSource;
import io.deephaven.db.v2.sources.regioned.RegionedPageStore;
import io.deephaven.db.v2.utils.CurrentOnlyIndex;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.parquet.ColumnChunkReader;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.RowGroupReader;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.*;
import java.util.stream.IntStream;

class ParquetTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private final ParquetInstructions readInstructions;
    private final ParquetFileReader parquetFileReader;
    private final int[] rowGroupIndices;

    private final RowGroup[] rowGroups;
    private final RegionedPageStore.Parameters regionParameters;
    private final Map<String, String[]> parquetColumnNameToPath;
    private final Map<String, GroupingColumnInfo> groupingColumns;
    private final Map<String, ColumnTypeInfo> columnTypes;

    private volatile RowGroupReader[] rowGroupReaders;

    ParquetTableLocation(@NotNull final TableKey tableKey,
                         @NotNull final ParquetTableLocationKey tableLocationKey,
                         @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, false);
        this.readInstructions = readInstructions;
        final ParquetMetadata parquetMetadata;
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (tableLocationKey) {
            parquetFileReader = tableLocationKey.getFileReader();
            parquetMetadata = tableLocationKey.getMetadata();
            rowGroupIndices = tableLocationKey.getRowGroupIndices();
        }

        final int rowGroupCount = rowGroupIndices.length;
        rowGroups = IntStream.of(rowGroupIndices)
                .mapToObj(rgi -> parquetFileReader.fileMetaData.getRow_groups().get(rgi))
                .sorted(Comparator.comparingInt(RowGroup::getOrdinal))
                .toArray(RowGroup[]::new);
        final long maxRowCount = Arrays.stream(rowGroups).mapToLong(RowGroup::getNum_rows).max().orElse(0L);
        regionParameters = new RegionedPageStore.Parameters(
                RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK, rowGroupCount, maxRowCount);

        parquetColumnNameToPath = new HashMap<>();
        for (final ColumnDescriptor column : parquetFileReader.getSchema().getColumns()) {
            final String[] path = column.getPath();
            if (path.length > 1) {
                parquetColumnNameToPath.put(path[0], path);
            }
        }

        // TODO (https://github.com/deephaven/deephaven-core/issues/958):
        //     When/if we support _metadata files for Deephaven-written Parquet tables, we may need to revise this
        //     in order to read *this* file's metadata, rather than inheriting file metadata from the _metadata file.
        //     Obvious issues included grouping table paths, codecs, etc.
        //     Presumably, we could store per-file instances of the metadata in the _metadata file's map.
        final Optional<TableInfo> tableInfo = ParquetSchemaReader.parseMetadata(parquetMetadata.getFileMetaData().getKeyValueMetaData());
        groupingColumns = tableInfo.map(TableInfo::groupingColumnMap).orElse(Collections.emptyMap());
        columnTypes = tableInfo.map(TableInfo::columnTypeMap).orElse(Collections.emptyMap());

        handleUpdate(computeIndex(), tableLocationKey.getFile().lastModified());
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {
    }

    File getParquetFile() {
        return ((ParquetTableLocationKey) getKey()).getFile();
    }

    ParquetInstructions getReadInstructions() {
        return readInstructions;
    }

    SeekableChannelsProvider getChannelProvider() {
        return parquetFileReader.getChannelsProvider();
    }

    RegionedPageStore.Parameters getRegionParameters() {
        return regionParameters;
    }

    public Map<String, GroupingColumnInfo> getGroupingColumns() {
        return groupingColumns;
    }

    public Map<String, ColumnTypeInfo> getColumnTypes() {
        return columnTypes;
    }

    private RowGroupReader[] getRowGroupReaders() {
        RowGroupReader[] local;
        if ((local = rowGroupReaders) != null) {
            return local;
        }
        synchronized (this) {
            if ((local = rowGroupReaders) != null) {
                return local;
            }
            return rowGroupReaders = IntStream.of(rowGroupIndices)
                    .mapToObj(parquetFileReader::getRowGroup)
                    .sorted(Comparator.comparingInt(rgr -> rgr.getRowGroup().getOrdinal()))
                    .toArray(RowGroupReader[]::new);
        }
    }

    @NotNull
    @Override
    protected ParquetColumnLocation<Values> makeColumnLocation(@NotNull final String columnName) {
        final String parquetColumnName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName);
        final String[] columnPath = parquetColumnNameToPath.get(parquetColumnName);
        final List<String> nameList = columnPath == null ? Collections.singletonList(parquetColumnName) : Arrays.asList(columnPath);
        final ColumnChunkReader[] columnChunkReaders = Arrays.stream(getRowGroupReaders()).map(rgr -> rgr.getColumnChunk(nameList)).toArray(ColumnChunkReader[]::new);
        final boolean exists = Arrays.stream(columnChunkReaders).anyMatch(ccr -> ccr != null && ccr.numRows() > 0);
        return new ParquetColumnLocation<>(this, columnName, parquetColumnName,
                exists ? columnChunkReaders : null,
                exists && groupingColumns.containsKey(parquetColumnName));
    }

    private CurrentOnlyIndex computeIndex() {
        final CurrentOnlyIndex.SequentialBuilder sequentialBuilder = Index.CURRENT_FACTORY.getSequentialBuilder();

        for (int rgi = 0; rgi < rowGroups.length; ++rgi) {
            final long subRegionSize = rowGroups[rgi].getNum_rows();
            final long subRegionFirstKey = (long) rgi << regionParameters.regionMaskNumBits;
            final long subRegionLastKey = subRegionFirstKey + subRegionSize - 1;
            sequentialBuilder.appendRange(subRegionFirstKey, subRegionLastKey);
        }
        return (CurrentOnlyIndex) sequentialBuilder.getIndex();
    }
}

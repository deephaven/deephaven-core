package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.util.file.TrackedFileHandleFactory;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.impl.AbstractTableLocation;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.regioned.RegionedColumnSource;
import io.deephaven.db.v2.sources.regioned.RegionedPageStore;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.parquet.ColumnChunkReader;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.RowGroupReader;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.utils.CachedChannelProvider;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.parquet.column.ColumnDescriptor;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;

class ParquetTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = ParquetColumnLocation.class.getSimpleName();

    private final File parquetFile;
    private final ParquetInstructions readInstructions;

    private final RowGroupReader[] rowGroupReaders;
    private final RegionedPageStore.Parameters regionParameters;
    private final Map<String, String[]> parquetColumnNameToPath;
    private final Map<String, String> keyValueMetaData;

    private final Set<String> groupingParquetColumnNames = new HashSet<>();

    private final SeekableChannelsProvider cachedChannelProvider = new CachedChannelProvider(new TrackedSeekableChannelsProvider(TrackedFileHandleFactory.getInstance()),
            Configuration.getInstance().getIntegerForClassWithDefault(ParquetTableLocation.class, "maxChannels", 100));

    ParquetTableLocation(@NotNull final TableKey tableKey,
                         @NotNull final TableLocationKey tableLocationKey,
                         final File parquetFile,
                         final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, false);
        this.readInstructions = readInstructions;
        this.parquetFile = parquetFile;
        int totalRows = 0;
        try {
            final ParquetFileReader parquetFileReader = new ParquetFileReader(parquetFile.getPath(), cachedChannelProvider, -1);

            final int rowGroupCount = parquetFileReader.fileMetaData.getRow_groups().size();
            rowGroupReaders = new RowGroupReader[rowGroupCount];
            long maxRowCount = 0;
            for (int rgi = 0; rgi < rowGroupCount; ++rgi) {
                final RowGroupReader reader = parquetFileReader.getRowGroup(rgi);
                rowGroupReaders[rgi] = reader;
                maxRowCount = Math.max(maxRowCount, reader.numRows());
                totalRows += reader.numRows();
            }
            regionParameters = new RegionedPageStore.Parameters(
                    RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK,  rowGroupCount, maxRowCount);

            parquetColumnNameToPath = new HashMap<>();
            for (final ColumnDescriptor column : parquetFileReader.getSchema().getColumns()) {
                final String[] path = column.getPath();
                if (path.length > 1) {
                    parquetColumnNameToPath.put(path[0], path);
                }
            }

            keyValueMetaData = new ParquetMetadataConverter().fromParquetMetadata(parquetFileReader.fileMetaData).getFileMetaData().getKeyValueMetaData();
            final String grouping = keyValueMetaData.get(ParquetTableWriter.GROUPING);
            if (grouping != null) {
                groupingParquetColumnNames.addAll(Arrays.asList(grouping.split(",")));
            }
        } catch (IOException e) {
            throw new TableDataException("Can't read parquet file " + parquetFile, e);
        }

        handleUpdate(totalRows, parquetFile.lastModified());
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {
    }

    File getParquetFile() {
        return parquetFile;
    }

    ParquetInstructions getReadInstructions() {
        return readInstructions;
    }

    SeekableChannelsProvider getChannelProvider() {
        return cachedChannelProvider;
    }

    public RegionedPageStore.Parameters getRegionParameters() {
        return regionParameters;
    }

    Map<String, String> getKeyValueMetaData() {
        return keyValueMetaData;
    }

    @NotNull
    @Override
    protected ParquetColumnLocation<Values> makeColumnLocation(@NotNull final String columnName) {
        final String parquetColumnName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName);
        final String[] columnPath = parquetColumnNameToPath.get(parquetColumnName);
        final List<String> nameList = columnPath == null ? Collections.singletonList(parquetColumnName) : Arrays.asList(columnPath);
        final ColumnChunkReader[] columnChunkReaders = Arrays.stream(rowGroupReaders).map(rgr -> rgr.getColumnChunk(nameList)).toArray(ColumnChunkReader[]::new);
        final boolean exists = Arrays.stream(columnChunkReaders).anyMatch(ccr -> ccr != null && ccr.numRows() > 0);
        return new ParquetColumnLocation<>(this, columnName, parquetColumnName,
                exists ? columnChunkReaders : null,
                exists && groupingParquetColumnNames.contains(parquetColumnName));
    }

    public void appendIndex(final long firstRegionKey, @NotNull final Index.SequentialBuilder sequentialBuilder) {
        
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.NullSelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.parquet.base.ColumnWriter;
import io.deephaven.parquet.base.NullParquetMetadataFileWriter;
import io.deephaven.parquet.base.ParquetMetadataFileWriter;
import io.deephaven.parquet.base.ParquetFileWriter;
import io.deephaven.parquet.base.ParquetUtils;
import io.deephaven.parquet.base.RowGroupWriter;
import io.deephaven.parquet.table.metadata.CodecInfo;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.metadata.DataIndexInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import io.deephaven.parquet.table.transfer.ArrayAndVectorTransfer;
import io.deephaven.parquet.table.transfer.StringDictionary;
import io.deephaven.parquet.table.transfer.TransferObject;
import io.deephaven.stringset.StringSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import io.deephaven.vector.Vector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.deephaven.parquet.base.ParquetUtils.METADATA_KEY;
import static io.deephaven.base.FileUtils.convertToURI;

/**
 * API for writing DH tables in parquet format
 */
public class ParquetTableWriter {
    public static final String GROUPING_KEY_COLUMN_NAME = "dh_key";
    public static final String GROUPING_BEGIN_POS_COLUMN_NAME = "dh_begin_pos";
    public static final String GROUPING_END_POS_COLUMN_NAME = "dh_end_pos";

    public static final String INDEX_ROW_SET_COLUMN_NAME = "dh_row_set";


    /**
     * Helper struct used to pass information about where to write the index files
     */
    static class IndexWritingInfo {
        /**
         * Names of the indexing key columns
         */
        final String[] indexColumnNames;
        /**
         * Parquet names of the indexing key columns
         */
        final String[] parquetColumnNames;
        /**
         * File path to be added in the index metadata of the main parquet file
         */
        final File destFileForMetadata;
        /**
         * Destination path for writing the index file. The two filenames can differ because we write index files to
         * shadow file paths first and then place them at the final path once the write is complete. The metadata should
         * always hold the accurate path.
         */
        final File destFile;

        IndexWritingInfo(
                final String[] indexColumnNames,
                final String[] parquetColumnNames,
                final File destFileForMetadata,
                final File destFile) {
            this.indexColumnNames = indexColumnNames;
            this.parquetColumnNames = parquetColumnNames;
            this.destFileForMetadata = destFileForMetadata.getAbsoluteFile();
            this.destFile = destFile.getAbsoluteFile();
        }
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t The table to write
     * @param definition Table definition
     * @param writeInstructions Write instructions for customizations while writing
     * @param destFilePath The destination path
     * @param destFilePathForMetadata The destination path to store in the metadata files. This can be different from
     *        {@code destFilePath} if we are writing the parquet file to a shadow location first since the metadata
     *        should always hold the accurate path.
     * @param incomingMeta A map of metadata values to be stores in the file footer
     * @param indexInfoList Arrays containing the column names for indexes to persist as sidecar tables. Indexes that
     *        are specified but missing will be computed on demand.
     * @param metadataFileWriter The writer for the {@value ParquetUtils#METADATA_FILE_NAME} and
     *        {@value ParquetUtils#COMMON_METADATA_FILE_NAME} files
     * @param computedCache When we need to perform some computation depending on column data to make a decision
     *        impacting both schema and written data, we store results in computedCache to avoid having to calculate
     *        twice. An example is the necessary precision and scale for a BigDecimal column written as a decimal
     *        logical type.
     *
     * @throws IOException For file writing related errors
     */
    static void write(
            @NotNull final Table t,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final String destFilePath,
            @NotNull final String destFilePathForMetadata,
            @NotNull final Map<String, String> incomingMeta,
            @Nullable final List<ParquetTableWriter.IndexWritingInfo> indexInfoList,
            @NotNull final ParquetMetadataFileWriter metadataFileWriter,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache) throws IOException {
        if (t.isRefreshing()) {
            /*
             * We mustn't write inconsistent tables or data indexes. This check is "basic". Snapshotting logic here
             * would probably be inappropriate, as we might be writing very large tables. Hopefully users aren't naively
             * writing Parquet tables from within listeners or transforms without ensuring proper dependency
             * satisfaction for the table and any indexes it has.
             */
            t.getUpdateGraph().checkInitiateSerialTableOperation();
        }

        final TableInfo.Builder tableInfoBuilder = TableInfo.builder();
        List<File> cleanupFiles = null;
        try {
            if (indexInfoList != null) {
                cleanupFiles = new ArrayList<>(indexInfoList.size());
                final Path destDirPath = new File(destFilePath).getAbsoluteFile().getParentFile().toPath();
                for (final ParquetTableWriter.IndexWritingInfo info : indexInfoList) {
                    try (final SafeCloseable ignored = t.isRefreshing() ? LivenessScopeStack.open() : null) {
                        // This will retrieve an existing index if one exists, or create a new one if not
                        final BasicDataIndex dataIndex = Optional
                                .ofNullable(DataIndexer.getDataIndex(t, info.indexColumnNames))
                                .or(() -> Optional.of(DataIndexer.getOrCreateDataIndex(t, info.indexColumnNames)))
                                .get()
                                .transform(DataIndexTransformer.builder().invertRowSet(t.getRowSet()).build());
                        final Table indexTable = dataIndex.table();

                        cleanupFiles.add(info.destFile);
                        tableInfoBuilder.addDataIndexes(DataIndexInfo.of(
                                destDirPath.relativize(info.destFileForMetadata.toPath()).toString(),
                                info.parquetColumnNames));
                        final ParquetInstructions writeInstructionsToUse;
                        if (INDEX_ROW_SET_COLUMN_NAME.equals(dataIndex.rowSetColumnName())) {
                            writeInstructionsToUse = writeInstructions;
                        } else {
                            writeInstructionsToUse = new ParquetInstructions.Builder(writeInstructions)
                                    .addColumnNameMapping(INDEX_ROW_SET_COLUMN_NAME, dataIndex.rowSetColumnName())
                                    .build();
                        }
                        write(indexTable, indexTable.getDefinition(), writeInstructionsToUse,
                                info.destFile.getAbsolutePath(), info.destFileForMetadata.getAbsolutePath(),
                                Collections.emptyMap(), TableInfo.builder(), NullParquetMetadataFileWriter.INSTANCE,
                                computedCache);
                    }
                }
            }
            write(t, definition, writeInstructions, destFilePath, destFilePathForMetadata, incomingMeta,
                    tableInfoBuilder, metadataFileWriter, computedCache);
        } catch (Exception e) {
            if (cleanupFiles != null) {
                for (final File cleanupFile : cleanupFiles) {
                    try {
                        // noinspection ResultOfMethodCallIgnored
                        cleanupFile.delete();
                    } catch (Exception ignored) {
                    }
                }
            }
            throw e;
        }
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param table The table to write
     * @param definition The table definition
     * @param writeInstructions Write instructions for customizations while writing
     * @param destFilePath The destination path
     * @param destFilePathForMetadata The destination path to store in the metadata files. This can be different from
     *        {@code destFilePath} if we are writing the parquet file to a shadow location first since the metadata
     *        should always hold the accurate path.
     * @param tableMeta A map of metadata values to be stores in the file footer
     * @param tableInfoBuilder A partially constructed builder for the metadata object
     * @param metadataFileWriter The writer for the {@value ParquetUtils#METADATA_FILE_NAME} and
     *        {@value ParquetUtils#COMMON_METADATA_FILE_NAME} files
     * @param computedCache Per column cache tags
     * @throws IOException For file writing related errors
     */
    static void write(
            @NotNull final Table table,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final String destFilePath,
            @NotNull final String destFilePathForMetadata,
            @NotNull final Map<String, String> tableMeta,
            @NotNull final TableInfo.Builder tableInfoBuilder,
            @NotNull final ParquetMetadataFileWriter metadataFileWriter,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache) throws IOException {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table t = pretransformTable(table, definition);
            final TrackingRowSet tableRowSet = t.getRowSet();
            final Map<String, ? extends ColumnSource<?>> columnSourceMap = t.getColumnSourceMap();
            final ParquetFileWriter parquetFileWriter = getParquetFileWriter(computedCache, definition, tableRowSet,
                    columnSourceMap, destFilePath, destFilePathForMetadata, writeInstructions, tableMeta,
                    tableInfoBuilder, metadataFileWriter);
            // Given the transformation, do not use the original table's "definition" for writing
            write(t, writeInstructions, parquetFileWriter, computedCache);
        }
    }

    /**
     * Writes a table in parquet format under a given path. This method should only be invoked when wrapped in a
     * try-with-resources using a new {@link LivenessScopeStack#open() LivenessScope} to ensure that the various derived
     * tables created are properly cleaned up.
     *
     * @param table The table to write
     * @param writeInstructions Write instructions for customizations while writing
     * @param parquetFileWriter the writer
     * @throws IOException For file writing related errors
     */
    private static void write(
            @NotNull final Table table,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final ParquetFileWriter parquetFileWriter,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache) throws IOException {
        final TrackingRowSet tableRowSet = table.getRowSet();
        final Map<String, ? extends ColumnSource<?>> columnSourceMap = table.getColumnSourceMap();
        final long nRows = table.size();
        if (nRows > 0) {
            final RowGroupWriter rowGroupWriter = parquetFileWriter.addRowGroup(nRows);
            for (final Map.Entry<String, ? extends ColumnSource<?>> nameToSource : columnSourceMap.entrySet()) {
                final String columnName = nameToSource.getKey();
                final ColumnSource<?> columnSource = nameToSource.getValue();
                try {
                    writeColumnSource(tableRowSet, writeInstructions, rowGroupWriter, computedCache, columnName,
                            columnSource);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to write column " + columnName, e);
                }
            }
        }

        parquetFileWriter.close();
    }

    /**
     * Get the parquet schema for a table
     *
     * @param table the input table
     * @param definition the definition to use for creating the schema
     * @param instructions write instructions for the file
     * @return the parquet schema
     */
    static MessageType getSchemaForTable(@NotNull final Table table,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions instructions) {
        if (definition.numColumns() == 0) {
            throw new IllegalArgumentException("Table definition must have at least one column");
        }
        final Table pretransformTable = pretransformTable(table, definition);
        return MappedSchema.create(new HashMap<>(), definition, pretransformTable.getRowSet(),
                pretransformTable.getColumnSourceMap(), instructions).getParquetSchema();
    }

    /**
     * Detect any missing or StringSet columns and convert them to arrays / null values as appropriate to prepare the
     * input table to be written to the parquet file.
     *
     * @param table the input table
     * @param definition the table definition being written
     * @return a transformed view of the input table. The table definition for the transformed view can be different
     *         from the definition of the input table.
     */
    @NotNull
    private static Table pretransformTable(@NotNull final Table table, @NotNull final TableDefinition definition) {
        final List<SelectColumn> updateViewColumnsTransform = new ArrayList<>();
        final List<SelectColumn> viewColumnsTransform = new ArrayList<>();

        for (final ColumnDefinition<?> column : definition.getColumns()) {
            final String colName = column.getName();
            if (table.hasColumns(colName)) {
                if (StringSet.class.isAssignableFrom(column.getDataType())) {
                    updateViewColumnsTransform.add(FormulaColumn.createFormulaColumn(colName,
                            "isNull(" + colName + ") ? null : " + colName + ".values()"));
                }
                viewColumnsTransform.add(new SourceColumn(colName));
            } else {
                // noinspection unchecked,rawtypes
                viewColumnsTransform.add(new NullSelectColumn(
                        column.getDataType(), column.getComponentType(), colName));
            }
        }

        Table transformed = table;
        if (!viewColumnsTransform.isEmpty()) {
            transformed = transformed.view(viewColumnsTransform);
        }

        if (!updateViewColumnsTransform.isEmpty()) {
            transformed = transformed.updateView(updateViewColumnsTransform);
        }
        return transformed;
    }

    /**
     * Create a {@link ParquetFileWriter} for writing the table to disk.
     *
     * @param computedCache Per column cache tags
     * @param definition the writable definition
     * @param tableRowSet the row set being written
     * @param columnSourceMap the columns of the table
     * @param destFilePath the destination to write to
     * @param destFilePathForMetadata The destination path to store in the metadata files. This can be different from
     *        {@code destFilePath} if we are writing the parquet file to a shadow location first since the metadata
     *        should always hold the accurate path.
     * @param writeInstructions write instructions for the file
     * @param tableMeta metadata to include in the parquet metadata
     * @param tableInfoBuilder a builder for accumulating per-column information to construct the deephaven metadata
     * @param metadataFileWriter The writer for the {@value ParquetUtils#METADATA_FILE_NAME} and
     *        {@value ParquetUtils#COMMON_METADATA_FILE_NAME} files
     *
     * @return a new file writer
     */
    @NotNull
    private static ParquetFileWriter getParquetFileWriter(
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final TableDefinition definition,
            @NotNull final RowSet tableRowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnSourceMap,
            @NotNull final String destFilePath,
            @NotNull final String destFilePathForMetadata,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final Map<String, String> tableMeta,
            @NotNull final TableInfo.Builder tableInfoBuilder,
            @NotNull final ParquetMetadataFileWriter metadataFileWriter) throws IOException {

        // First, map the TableDefinition to a parquet Schema
        final MappedSchema mappedSchema =
                MappedSchema.create(computedCache, definition, tableRowSet, columnSourceMap, writeInstructions);
        for (final ColumnDefinition<?> column : definition.getColumns()) {
            final String colName = column.getName();
            final ColumnTypeInfo.Builder columnInfoBuilder = ColumnTypeInfo.builder()
                    .columnName(writeInstructions.getParquetColumnNameFromColumnNameOrDefault(colName));
            boolean usedColumnInfo = false;
            final Pair<String, String> codecData = TypeInfos.getCodecAndArgs(column, writeInstructions);
            if (codecData != null) {
                final CodecInfo.Builder codecInfoBuilder = CodecInfo.builder();
                codecInfoBuilder.codecName(codecData.getLeft());
                final String codecArg = codecData.getRight();
                if (codecArg != null) {
                    codecInfoBuilder.codecArg(codecArg);
                }
                codecInfoBuilder.dataType(column.getDataType().getName());
                final Class<?> componentType = column.getComponentType();
                if (componentType != null) {
                    codecInfoBuilder.componentType(componentType.getName());
                }
                columnInfoBuilder.codec(codecInfoBuilder.build());
                usedColumnInfo = true;
            }

            if (StringSet.class.isAssignableFrom(column.getDataType())) {
                columnInfoBuilder.specialType(ColumnTypeInfo.SpecialType.StringSet);
                usedColumnInfo = true;
            } else if (Vector.class.isAssignableFrom(column.getDataType())) {
                columnInfoBuilder.specialType(ColumnTypeInfo.SpecialType.Vector);
                usedColumnInfo = true;
            }

            if (usedColumnInfo) {
                tableInfoBuilder.addColumnTypes(columnInfoBuilder.build());
            }
        }

        final Map<String, String> extraMetaData = new HashMap<>(tableMeta);
        extraMetaData.put(METADATA_KEY, tableInfoBuilder.build().serializeToJSON());
        return new ParquetFileWriter(destFilePath, destFilePathForMetadata,
                SeekableChannelsProviderLoader.getInstance().fromServiceLoader(convertToURI(destFilePath, false), null),
                writeInstructions.getTargetPageSize(),
                new HeapByteBufferAllocator(), mappedSchema.getParquetSchema(),
                writeInstructions.getCompressionCodecName(), extraMetaData, metadataFileWriter);
    }

    @VisibleForTesting
    static <DATA_TYPE> void writeColumnSource(
            @NotNull final RowSet tableRowSet,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final RowGroupWriter rowGroupWriter,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final String columnName,
            @NotNull ColumnSource<DATA_TYPE> columnSource) throws IllegalAccessException, IOException {
        try (final ColumnWriter columnWriter = rowGroupWriter.addColumn(
                writeInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName))) {
            boolean usedDictionary = false;
            if (String.class.equals(columnSource.getType()) || String.class.equals(columnSource.getComponentType())) {
                usedDictionary =
                        tryEncodeDictionary(tableRowSet, writeInstructions, columnWriter, columnName, columnSource);
            }
            if (!usedDictionary) {
                encodePlain(tableRowSet, writeInstructions, columnWriter, computedCache, columnName, columnSource);
            }
        }
    }

    /**
     * Makes a copy of the given buffer
     */
    private static IntBuffer makeCopy(IntBuffer orig) {
        IntBuffer copy = IntBuffer.allocate(orig.capacity());
        copy.put(orig).flip();
        return copy;
    }

    private static <DATA_TYPE> boolean tryEncodeDictionary(
            @NotNull final RowSet tableRowSet,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final String columnName,
            @NotNull final ColumnSource<DATA_TYPE> columnSource) throws IOException {
        final boolean useDictionaryHint = writeInstructions.useDictionary(columnName);
        final int maxKeys = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionaryKeys();
        final int maxDictSize = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionarySize();
        // We encode dictionary positions as integers, therefore for a null string, we use NULL_INT as the position
        final int NULL_POS = QueryConstants.NULL_INT;
        final Statistics<?> statistics = columnWriter.getStats();
        final List<IntBuffer> pageBuffers = new ArrayList<>();
        final List<IntBuffer> lengthsBuffers = new ArrayList<>();
        final BitSet pageBufferHasNull = new BitSet();
        final boolean isArrayOrVector = (columnSource.getComponentType() != null);
        final StringDictionary dictionary = new StringDictionary(maxKeys, maxDictSize, statistics, NULL_POS);
        int curPage = 0;
        try (final TransferObject<IntBuffer> transferObject = TransferObject.createDictEncodedStringTransfer(
                tableRowSet, columnSource, writeInstructions.getTargetPageSize(), dictionary)) {
            boolean done;
            do {
                // Paginate the data and prepare the dictionary. Then add the dictionary page followed by all data pages
                transferObject.transferOnePageToBuffer();
                done = !transferObject.hasMoreDataToBuffer();
                if (done) {
                    // If done, we store a reference to transfer object's page buffer, else we make copies of all the
                    // page buffers and write them later
                    pageBuffers.add(transferObject.getBuffer());
                    if (isArrayOrVector) {
                        lengthsBuffers.add(transferObject.getRepeatCount());
                    }
                } else {
                    pageBuffers.add(makeCopy(transferObject.getBuffer()));
                    if (isArrayOrVector) {
                        lengthsBuffers.add(makeCopy(transferObject.getRepeatCount()));
                    }
                }
                if (transferObject.pageHasNull()) {
                    pageBufferHasNull.set(curPage);
                }
                curPage++;
            } while (!done);
        } catch (final DictionarySizeExceededException ignored) {
            // Reset the stats because we will re-encode these in PLAIN encoding.
            columnWriter.resetStats();
            // TODO(deephaven-core#946): We discard all dictionary data accumulated so far and fall back to PLAIN
            // encoding. We could have added a dictionary page first with data collected so far and then encoded the
            // remaining data using PLAIN encoding
            return false;
        }

        if (dictionary.getKeyCount() == 0 && !pageBufferHasNull.isEmpty()) {
            // Reset the stats because we will re-encode these in PLAIN encoding.
            columnWriter.resetStats();
            return false;
        }
        columnWriter.addDictionaryPage(dictionary.getEncodedKeys(), dictionary.getKeyCount());
        // We've already determined min/max statistics for the strings while building the dictionary. The buffer now
        // stores only the offsets in the dictionary, and we don't need statistics for offsets. Therefore, we create a
        // temporary integer stats object just to track the number of nulls and pass it to lower layers.
        // We use the following fake type object to create proper statistics object
        final PrimitiveType fakeObject = Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("fake");
        final Statistics<?> tmpStats = Statistics.createStats(fakeObject);
        final int numPages = pageBuffers.size();
        for (int i = 0; i < numPages; ++i) {
            final IntBuffer pageBuffer = pageBuffers.get(i);
            if (isArrayOrVector) {
                columnWriter.addVectorPage(pageBuffer, lengthsBuffers.get(i), pageBuffer.remaining(), tmpStats);
            } else {
                final boolean pageHasNulls = pageBufferHasNull.get(i);
                if (pageHasNulls) {
                    columnWriter.addPage(pageBuffer, pageBuffer.remaining(), tmpStats);
                } else {
                    columnWriter.addPageNoNulls(pageBuffer, pageBuffer.remaining(), tmpStats);
                }
            }
        }
        // Add the count of nulls to the overall stats.
        statistics.incrementNumNulls(tmpStats.getNumNulls());
        return true;
    }

    private static <DATA_TYPE> void encodePlain(
            @NotNull final RowSet tableRowSet,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final String columnName,
            @NotNull final ColumnSource<DATA_TYPE> columnSource) throws IOException {
        try (final TransferObject<?> transferObject = TransferObject.create(
                tableRowSet, writeInstructions, computedCache, columnName, columnSource)) {
            final Statistics<?> statistics = columnWriter.getStats();
            boolean writeVectorPages = (transferObject instanceof ArrayAndVectorTransfer);
            do {
                int numValuesBuffered = transferObject.transferOnePageToBuffer();
                if (writeVectorPages) {
                    columnWriter.addVectorPage(transferObject.getBuffer(), transferObject.getRepeatCount(),
                            numValuesBuffered, statistics);
                } else {
                    columnWriter.addPage(transferObject.getBuffer(), numValuesBuffered, statistics);
                }
            } while (transferObject.hasMoreDataToBuffer());
        }
    }
}

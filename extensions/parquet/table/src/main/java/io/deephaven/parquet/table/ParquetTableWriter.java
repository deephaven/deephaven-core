/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import gnu.trove.impl.Constants;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.CodecLookup;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.NullSelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.parquet.base.ColumnWriter;
import io.deephaven.parquet.base.ParquetFileWriter;
import io.deephaven.parquet.base.RowGroupWriter;
import io.deephaven.parquet.table.metadata.CodecInfo;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.metadata.GroupingColumnInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import io.deephaven.parquet.table.transfer.*;
import io.deephaven.parquet.table.util.TrackedSeekableChannelsProvider;
import io.deephaven.stringset.StringSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.vector.Vector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.IntSupplier;

/**
 * API for writing DH tables in parquet format
 */
public class ParquetTableWriter {
    private static final int INITIAL_DICTIONARY_SIZE = 1 << 8;
    public static final String METADATA_KEY = "deephaven";
    public static final String BEGIN_POS = "dh_begin_pos";
    public static final String END_POS = "dh_end_pos";
    public static final String GROUPING_KEY = "dh_key";
    public static final String PARQUET_FILE_EXTENSION = ".parquet";

    /**
     * Helper struct used to pass information about where to write the grouping files for each grouping column
     */
    public static class GroupingColumnWritingInfo {
        /**
         * Parquet name of this grouping column
         */
        public final String parquetColumnName;
        /**
         * File path to be added in the grouping metadata of main parquet file
         */
        public final File metadataFilePath;

        /**
         * Destination path for writing the grouping file. The two filenames can differ because we write grouping files
         * to shadow file paths first and then place them at the final path once the write is complete. But the metadata
         * should always hold the accurate path.
         */
        public final File destFile;

        public GroupingColumnWritingInfo(final String parquetColumnName, final File metadataFilePath,
                final File destFile) {
            this.parquetColumnName = parquetColumnName;
            this.metadataFilePath = metadataFilePath;
            this.destFile = destFile;
        }
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t The table to write
     * @param definition Table definition
     * @param writeInstructions Write instructions for customizations while writing
     * @param destPathName The destination path
     * @param incomingMeta A map of metadata values to be stores in the file footer
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to
     *         unsupported types)
     * @throws IOException For file writing related errors
     */
    public static void write(
            @NotNull final Table t,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final String destPathName,
            @NotNull final Map<String, String> incomingMeta,
            final Map<String, GroupingColumnWritingInfo> groupingColumnsWritingInfoMap)
            throws SchemaMappingException, IOException {
        final TableInfo.Builder tableInfoBuilder = TableInfo.builder();
        List<File> cleanupFiles = null;
        try {
            if (groupingColumnsWritingInfoMap != null) {
                cleanupFiles = new ArrayList<>(groupingColumnsWritingInfoMap.size());
                final Path destDirPath = Paths.get(destPathName).getParent();
                for (Map.Entry<String, GroupingColumnWritingInfo> entry : groupingColumnsWritingInfoMap.entrySet()) {
                    final String groupingColumnName = entry.getKey();
                    final Table auxiliaryTable = groupingAsTable(t, groupingColumnName);
                    final String parquetColumnName = entry.getValue().parquetColumnName;
                    final File metadataFilePath = entry.getValue().metadataFilePath;
                    final File groupingDestFile = entry.getValue().destFile;
                    cleanupFiles.add(groupingDestFile);
                    tableInfoBuilder.addGroupingColumns(GroupingColumnInfo.of(parquetColumnName,
                            destDirPath.relativize(metadataFilePath.toPath()).toString()));
                    write(auxiliaryTable, auxiliaryTable.getDefinition(), writeInstructions,
                            groupingDestFile.getAbsolutePath(), Collections.emptyMap(), TableInfo.builder());
                }
            }
            write(t, definition, writeInstructions, destPathName, incomingMeta, tableInfoBuilder);
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
     * @param path The destination path
     * @param tableMeta A map of metadata values to be stores in the file footer
     * @param tableInfoBuilder A partially-constructed builder for the metadata object
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to
     *         unsupported types)
     * @throws IOException For file writing related errors
     */
    public static void write(
            @NotNull final Table table,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final String path,
            @NotNull final Map<String, String> tableMeta,
            @NotNull final TableInfo.Builder tableInfoBuilder) throws SchemaMappingException, IOException {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table t = pretransformTable(table, definition);
            final TrackingRowSet tableRowSet = t.getRowSet();
            final Map<String, ? extends ColumnSource<?>> columnSourceMap = t.getColumnSourceMap();
            // When we need to perform some computation depending on column data to make a decision impacting both
            // schema and written data, we store results in computedCache to avoid having to calculate twice.
            // An example is the necessary precision and scale for a BigDecimal column writen as decimal logical type.
            final Map<String, Map<ParquetCacheTags, Object>> computedCache = new HashMap<>();
            final ParquetFileWriter parquetFileWriter = getParquetFileWriter(computedCache, definition, tableRowSet,
                    columnSourceMap, path, writeInstructions, tableMeta,
                    tableInfoBuilder);

            write(t, definition, writeInstructions, parquetFileWriter, computedCache);
        }
    }

    /**
     * Writes a table in parquet format under a given path. This method should only be invoked when wrapped in a
     * try-with-resources using a new {@link LivenessScopeStack#open() LivenessScope} to ensure that the various derived
     * tables created are properly cleaned up.
     *
     * @param table The table to write
     * @param definition The table definition
     * @param writeInstructions Write instructions for customizations while writing
     * @param parquetFileWriter the writer
     * @throws IOException For file writing related errors
     */
    private static void write(
            @NotNull final Table table,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final ParquetFileWriter parquetFileWriter,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache) throws IOException {
        final TrackingRowSet tableRowSet = table.getRowSet();
        final Map<String, ? extends ColumnSource<?>> columnSourceMap = table.getColumnSourceMap();
        final long nRows = table.size();
        if (nRows > 0) {
            final RowGroupWriter rowGroupWriter = parquetFileWriter.addRowGroup(nRows);
            for (final Map.Entry<String, ? extends ColumnSource<?>> nameToSource : columnSourceMap.entrySet()) {
                final String name = nameToSource.getKey();
                final ColumnSource<?> columnSource = nameToSource.getValue();
                try {
                    writeColumnSource(computedCache, tableRowSet, rowGroupWriter, name, columnSource,
                            definition.getColumn(name), writeInstructions);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to write column " + name, e);
                }
            }
        }

        parquetFileWriter.close();
    }

    /**
     * Detect any missing or StringSet columns and convert them to arrays / null values as appropriate to prepare the
     * input table to be written to the parquet file.
     *
     * @param table the input table
     * @param definition the table definition being written
     * @return a transformed view of the input table.
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
     * @param path the destination to write to
     * @param writeInstructions write instructions for the file
     * @param tableMeta metadata to include in the parquet metadata
     * @param tableInfoBuilder a builder for accumulating per-column information to construct the deephaven metadata
     *
     * @return a new file writer
     */
    @NotNull
    private static ParquetFileWriter getParquetFileWriter(
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final TableDefinition definition,
            @NotNull final RowSet tableRowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnSourceMap,
            @NotNull final String path,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final Map<String, String> tableMeta,
            @NotNull final TableInfo.Builder tableInfoBuilder) throws IOException {

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
        return new ParquetFileWriter(path, TrackedSeekableChannelsProvider.getInstance(),
                writeInstructions.getTargetPageSize(),
                new HeapByteBufferAllocator(), mappedSchema.getParquetSchema(),
                writeInstructions.getCompressionCodecName(), extraMetaData);
    }

    @VisibleForTesting
    static <DATA_TYPE> void writeColumnSource(
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final RowSet tableRowSet,
            @NotNull final RowGroupWriter rowGroupWriter,
            @NotNull final String name,
            @NotNull ColumnSource<DATA_TYPE> columnSourceIn,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final ParquetInstructions writeInstructions) throws IllegalAccessException, IOException {
        Class<DATA_TYPE> columnType = columnSourceIn.getType();
        if (columnType == Instant.class) {
            // noinspection unchecked
            columnSourceIn = (ColumnSource<DATA_TYPE>) ReinterpretUtils.instantToLongSource(
                    (ColumnSource<Instant>) columnSourceIn);
            columnType = columnSourceIn.getType();
        } else if (columnType == Boolean.class) {
            // noinspection unchecked
            columnSourceIn = (ColumnSource<DATA_TYPE>) ReinterpretUtils.booleanToByteSource(
                    (ColumnSource<Boolean>) columnSourceIn);
        }

        try (final ColumnWriter columnWriter = rowGroupWriter.addColumn(
                writeInstructions.getParquetColumnNameFromColumnNameOrDefault(name))) {
            boolean usedDictionary = false;
            if (columnSourceIn.getType() == String.class) {
                usedDictionary = tryEncodeDictionary(writeInstructions, tableRowSet, columnDefinition, columnWriter,
                        columnSourceIn);
            }
            if (!usedDictionary) {
                encodePlain(writeInstructions, tableRowSet, columnDefinition, columnType, columnWriter, columnSourceIn,
                        computedCache);
            }
        }
    }

    private static <DATA_TYPE> void encodePlain(@NotNull final ParquetInstructions writeInstructions,
            @NotNull final RowSet tableRowSet,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final Class<DATA_TYPE> columnType,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final ColumnSource<DATA_TYPE> columnSourceIn,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache) throws IOException {
        // TODO Can move tableRowSetIt construction code from inside TransferObject to here in try-with-resources block
        try (final TransferObject<?> transferObject = TransferObject.create(computedCache,
                tableRowSet,
                columnSourceIn,
                columnDefinition,
                columnType,
                writeInstructions)) {
            final Statistics<?> statistics = columnWriter.getStats();
            boolean writeVectorPages = columnSourceIn.getComponentType() != null
                    && !CodecLookup.explicitCodecPresent(writeInstructions.getCodecName(columnDefinition.getName()))
                    && !CodecLookup.codecRequired(columnDefinition);
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

    private static <DATA_TYPE> boolean tryEncodeDictionary(
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final RowSet tableRowSet,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final ColumnSource<DATA_TYPE> columnSourceIn) throws IOException {
        // Note: We only support strings as dictionary pages. Knowing that, we can make some assumptions about chunk
        // types and avoid a bunch of lambda and virtual method invocations. If we decide to support more, than
        // these assumptions will need to be revisited.
        Assert.eq(columnDefinition.getDataType(), "columnDefinition.getDataType()", String.class, "String.class");

        // TODO Add support for vector of strings

        final boolean useDictionaryHint = writeInstructions.useDictionary(columnDefinition.getName());
        final int maxKeys = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionaryKeys();
        final int maxDictSize = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionarySize();
        final Statistics<?> statistics = columnWriter.getStats();
        try {
            final List<IntBuffer> pageBuffers = new ArrayList<>();
            final BitSet pageBufferHasNull = new BitSet();
            Binary[] encodedKeys = new Binary[Math.min(INITIAL_DICTIONARY_SIZE, maxKeys)];

            final TObjectIntHashMap<String> keyToPos =
                    new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY,
                            Constants.DEFAULT_LOAD_FACTOR,
                            QueryConstants.NULL_INT);
            int keyCount = 0;
            int dictSize = 0;
            boolean hasNulls = false;
            int maxValuesPerPage = writeInstructions.getTargetPageSize() / Integer.BYTES; // Because we will encode the
                                                                                          // strings as integer
                                                                                          // dictionary positions
            try (final ChunkSource.GetContext context = columnSourceIn.makeGetContext(maxValuesPerPage);
                    final RowSequence.Iterator tableRowSetIt = tableRowSet.getRowSequenceIterator()) {
                int curPage = 0;
                do {
                    boolean pageHasNulls = false;
                    final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength(maxValuesPerPage);
                    final ObjectChunk<String, ? extends Values> chunk =
                            columnSourceIn.getChunk(context, rs).asObjectChunk();
                    final IntBuffer posInDictionary = IntBuffer.allocate(rs.intSize());
                    for (int vi = 0; vi < chunk.size(); ++vi) {
                        final String key = chunk.get(vi);
                        int dictionaryPos = keyToPos.get(key);
                        if (dictionaryPos == keyToPos.getNoEntryValue()) {
                            // Track the min/max statistics while the dictionary is being built.
                            if (key == null) {
                                hasNulls = pageHasNulls = true;
                            } else {
                                if (keyCount == encodedKeys.length) {
                                    // Copy into an array of double the size with upper limit at maxKeys
                                    if (keyCount == maxKeys) {
                                        throw new DictionarySizeExceededException(String.format(
                                                "Dictionary maximum keys exceeded for %s", columnDefinition.getName()));
                                    }
                                    encodedKeys = Arrays.copyOf(encodedKeys, (int) Math.min(keyCount * 2L, maxKeys));
                                }
                                final Binary encodedKey = Binary.fromString(key);
                                dictSize += encodedKey.length();
                                if (dictSize > maxDictSize) {
                                    throw new DictionarySizeExceededException(
                                            String.format("Dictionary maximum size exceeded for %s",
                                                    columnDefinition.getName()));
                                }
                                encodedKeys[keyCount] = encodedKey;
                                statistics.updateStats(encodedKey);
                                dictionaryPos = keyCount;
                                keyCount++;
                            }
                            keyToPos.put(key, dictionaryPos);
                        }
                        posInDictionary.put(dictionaryPos);
                    }
                    pageBuffers.add(posInDictionary);
                    pageBufferHasNull.set(curPage, pageHasNulls);
                    curPage++;
                } while (tableRowSetIt.hasMore());
            }

            if (keyCount == 0 && hasNulls) {
                // Reset the stats because we will re-encode these in PLAIN encoding.
                columnWriter.resetStats();
                return false;
            }

            columnWriter.addDictionaryPage(encodedKeys, keyCount);
            // We've already determined min/max statistics while building the dictionary. Now use an integer statistics
            // object to track the number of nulls that will be written.
            Statistics<Integer> tmpStats = new IntStatistics();
            for (int i = 0; i < pageBuffers.size(); ++i) {
                final IntBuffer pageBuffer = pageBuffers.get(i);
                final boolean pageHasNulls = pageBufferHasNull.get(i);
                pageBuffer.flip();
                if (pageHasNulls) {
                    columnWriter.addPage(pageBuffer, pageBuffer.remaining(), tmpStats);
                } else {
                    columnWriter.addPageNoNulls(pageBuffer, pageBuffer.remaining(), tmpStats);
                }
            }
            // Add the count of nulls to the overall stats.
            statistics.incrementNumNulls(tmpStats.getNumNulls());
            return true;
        } catch (final DictionarySizeExceededException ignored) {
            // Reset the stats because we will re-encode these in PLAIN encoding.
            columnWriter.resetStats();
            // We discard all the dictionary data accumulated so far and fall back to PLAIN encoding. We could have
            // added a dictionary page first with data collected so far and then encoded the remaining data using PLAIN
            // encoding (TODO deephaven-core#946).
            return false;
        }
    }

    private static Table groupingAsTable(Table tableToSave, String columnName) {
        final QueryTable coalesced = (QueryTable) tableToSave.coalesce();
        final Table tableToGroup = (coalesced.isRefreshing() ? (QueryTable) coalesced.silent() : coalesced)
                .withAttributes(Map.of(Table.BLINK_TABLE_ATTRIBUTE, true)); // We want persistent first/last-by
        final Table grouped = tableToGroup
                .view(List.of(Selectable.of(ColumnName.of(GROUPING_KEY), ColumnName.of(columnName)),
                        Selectable.of(ColumnName.of(BEGIN_POS), RawString.of("ii")), // Range start, inclusive
                        Selectable.of(ColumnName.of(END_POS), RawString.of("ii+1")))) // Range end, exclusive
                .aggBy(List.of(Aggregation.AggFirst(BEGIN_POS), Aggregation.AggLast(END_POS)),
                        List.of(ColumnName.of(GROUPING_KEY)));
        final Table invalid = grouped.where(BEGIN_POS + " != 0 && " + BEGIN_POS + " != " + END_POS + "_[ii-1]");
        if (!invalid.isEmpty()) {
            throw new UncheckedDeephavenException(
                    "Range grouping is not possible for column because some indices are not contiguous");
        }
        return grouped;
    }
}

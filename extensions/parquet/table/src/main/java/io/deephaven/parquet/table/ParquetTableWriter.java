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
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.parquet.base.ColumnWriter;
import io.deephaven.parquet.base.ParquetFileWriter;
import io.deephaven.parquet.base.RowGroupWriter;
import io.deephaven.parquet.table.metadata.CodecInfo;
import io.deephaven.parquet.table.metadata.ColumnTypeInfo;
import io.deephaven.parquet.table.metadata.GroupingColumnInfo;
import io.deephaven.parquet.table.metadata.TableInfo;
import io.deephaven.parquet.table.util.TrackedSeekableChannelsProvider;
import io.deephaven.stringset.StringSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.IntSupplier;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * API for writing DH tables in parquet format
 */
public class ParquetTableWriter {
    private static final int INITIAL_DICTIONARY_SIZE = 1 << 8;

    public static final String METADATA_KEY = "deephaven";

    private static final int LOCAL_CHUNK_SIZE = 1024;

    public static final String BEGIN_POS = "dh_begin_pos";
    public static final String END_POS = "dh_end_pos";
    public static final String GROUPING_KEY = "dh_key";

    public static final String PARQUET_FILE_EXTENSION = ".parquet";

    enum CacheTags {
        DECIMAL_ARGS
    }

    /**
     * Classes that implement this interface are responsible for converting data from individual DH columns into buffers
     * to be written out to the Parquet file.
     *
     * @param <B>
     */
    interface TransferObject<B> extends SafeCloseable {
        /**
         * Fetch all data corresponding to the provided row sequence.
         */
        void fetchData(RowSequence rs);

        /**
         * Transfer all the fetched data into an internal buffer, which can then be accessed using
         * {@link TransferObject#getBuffer()}. This method should only be called after
         * {@link TransferObject#fetchData(RowSequence)}}. This method should be used when writing unpaginated data, and
         * should not be interleaved with calls to {@link TransferObject#transferOnePageToBuffer()}. Note that this
         * method can lead to out-of-memory error for variable-width types (e.g. strings) if the fetched data is too
         * big.
         *
         * @return The number of fetched data entries copied into the buffer.
         */
        int transferAllToBuffer();

        /**
         * Transfer one page size worth of fetched data into an internal buffer, which can then be accessed using
         * {@link TransferObject#getBuffer()}. The target page size is passed in the constructor. The method should only
         * be called after {@link TransferObject#fetchData(RowSequence)}}. This method should be used when writing
         * paginated data, and should not be interleaved with calls to {@link TransferObject#transferAllToBuffer()}.
         *
         * @return The number of fetched data entries copied into the buffer. This can be different from the total
         *         number of entries fetched in case of variable-width types (e.g. strings) where we enforce additional
         *         page size limits while copying.
         */
        int transferOnePageToBuffer();

        /**
         * Check if there is any fetched data which can be copied into buffer
         */
        boolean hasMoreDataToBuffer();

        B getBuffer();
    }

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
            final Map<String, Map<CacheTags, Object>> computedCache = new HashMap<>();
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
            @NotNull final Map<String, Map<CacheTags, Object>> computedCache) throws IOException {
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
     * @return a new file writer
     */
    @NotNull
    private static ParquetFileWriter getParquetFileWriter(
            @NotNull final Map<String, Map<CacheTags, Object>> computedCache,
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

    private interface ColumnWriteHelper {

        boolean isVectorFormat();

        IntSupplier valuePageSizeSupplier();
    }

    /**
     * ColumnWriteHelper for columns of "flat" data with no nesting or vector encoding.
     */
    private static class FlatColumnWriterHelper implements ColumnWriteHelper {

        /**
         * The maximum page size for values.
         */
        private final int maxValuePageSize;

        private FlatColumnWriterHelper(final int maxValuePageSize) {
            this.maxValuePageSize = maxValuePageSize;
        }

        public boolean isVectorFormat() {
            return false;
        }

        public IntSupplier valuePageSizeSupplier() {
            return () -> maxValuePageSize;
        }
    }

    /**
     * This is a helper struct storing useful data required to write column source in the parquet file, particularly
     * helpful for writing array/vector data.
     */
    private static class VectorColumnWriterHelper implements ColumnWriteHelper {

        /**
         * The source for per-row array/vector lengths.
         */
        private final ColumnSource<?> lengthSource;

        /**
         * The RowSet for (ungrouped) values.
         */
        private final RowSet valueRowSet;

        /**
         * The size of each value page. Parallel to {@link #lengthPageSizes}.
         */
        private final TIntArrayList valuePageSizes;

        /**
         * The size of each length page. Parallel to {@link #valuePageSizes}.
         */
        private final TIntArrayList lengthPageSizes;

        private VectorColumnWriterHelper(
                @NotNull final ColumnSource<?> lengthSource,
                @NotNull final RowSet valueRowSet) {
            this.lengthSource = lengthSource;
            this.valueRowSet = valueRowSet;
            valuePageSizes = new TIntArrayList();
            lengthPageSizes = new TIntArrayList();
        }

        public boolean isVectorFormat() {
            return true;
        }

        public IntSupplier lengthPageSizeSupplier() {
            return lengthPageSizes.iterator()::next;
        }

        public IntSupplier valuePageSizeSupplier() {
            return valuePageSizes.iterator()::next;
        }
    }

    @VisibleForTesting
    static <DATA_TYPE> void writeColumnSource(
            @NotNull final Map<String, Map<CacheTags, Object>> computedCache,
            @NotNull final TrackingRowSet tableRowSet,
            @NotNull final RowGroupWriter rowGroupWriter,
            @NotNull final String name,
            @NotNull final ColumnSource<DATA_TYPE> columnSourceIn,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final ParquetInstructions writeInstructions) throws IllegalAccessException, IOException {
        ColumnSource<DATA_TYPE> valueSource = columnSourceIn;

        final ColumnWriteHelper helper;
        int maxValuesPerPage = 0;
        int maxRowsPerPage = 0;
        int pageCount;
        if (columnSourceIn.getComponentType() != null
                && !CodecLookup.explicitCodecPresent(writeInstructions.getCodecName(columnDefinition.getName()))
                && !CodecLookup.codecRequired(columnDefinition)) {
            final VectorColumnWriterHelper vectorHelper;
            final int targetValuesPerPage = getTargetRowsPerPage(
                    valueSource.getComponentType(),
                    writeInstructions.getTargetPageSize());
            final HashMap<String, ColumnSource<?>> columns = new HashMap<>();
            columns.put("array", valueSource);
            {
                final Table lengthsTable = new QueryTable(tableRowSet, columns);
                final ColumnSource<?> lengthSource = lengthsTable
                        .view("len= ((Object)array) == null ? null : (int)array."
                                + (Vector.class.isAssignableFrom(valueSource.getType()) ? "size()" : "length"))
                        .getColumnSource("len");
                final Table ungroupedArrays = lengthsTable.ungroup("array");
                vectorHelper = new VectorColumnWriterHelper(lengthSource, ungroupedArrays.getRowSet());
                helper = vectorHelper;
                valueSource = ungroupedArrays.getColumnSource("array");
            }

            // This is the count of items contained in all arrays from the original table as we process.
            int valuesInPage = 0;

            // This is the count of rows in the original table as we process them
            int rowsInPage = 0;
            try (final ChunkSource.GetContext context = vectorHelper.lengthSource.makeGetContext(LOCAL_CHUNK_SIZE);
                    final RowSequence.Iterator it = tableRowSet.getRowSequenceIterator()) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(LOCAL_CHUNK_SIZE);
                    final IntChunk<? extends Values> lengthChunk =
                            vectorHelper.lengthSource.getChunk(context, rs).asIntChunk();
                    for (int chunkPos = 0; chunkPos < lengthChunk.size(); chunkPos++) {
                        final int curLength = lengthChunk.get(chunkPos);
                        if (curLength != NULL_INT) {
                            // If this array puts us past the target number of items within a page then we'll record the
                            // current values into the page lists above and restart our counts.
                            if ((valuesInPage + curLength > targetValuesPerPage || rowsInPage + 1 > targetValuesPerPage)
                                    && (valuesInPage > 0 || rowsInPage > 0)) {
                                // Record the current item count and original row count into the parallel page arrays.
                                vectorHelper.valuePageSizes.add(valuesInPage);
                                vectorHelper.lengthPageSizes.add(rowsInPage);
                                maxValuesPerPage = Math.max(valuesInPage, maxValuesPerPage);
                                maxRowsPerPage = Math.max(rowsInPage, maxRowsPerPage);

                                // Reset the counts to compute these values for the next page.
                                rowsInPage = 0;
                                valuesInPage = 0;
                            }
                            valuesInPage += curLength;
                        }
                        rowsInPage++;
                    }
                }
            }

            // If there are any leftover, accumulate the last page.
            if (rowsInPage > 0) {
                maxValuesPerPage = Math.max(valuesInPage, maxValuesPerPage);
                maxRowsPerPage = Math.max(rowsInPage, maxRowsPerPage);
                vectorHelper.valuePageSizes.add(valuesInPage);
                vectorHelper.lengthPageSizes.add(rowsInPage);
            }
            pageCount = vectorHelper.valuePageSizes.size();
        } else {
            final long tableSize = tableRowSet.size();
            final int targetPageSize = getTargetRowsPerPage(
                    valueSource.getType(), writeInstructions.getTargetPageSize());
            maxValuesPerPage = maxRowsPerPage = (int) Math.min(tableSize, targetPageSize);
            helper = new FlatColumnWriterHelper(maxValuesPerPage);
            pageCount = Math.toIntExact((tableSize + targetPageSize - 1) / targetPageSize);
        }

        Class<DATA_TYPE> columnType = valueSource.getType();
        if (columnType == Instant.class) {
            // noinspection unchecked
            valueSource = (ColumnSource<DATA_TYPE>) ReinterpretUtils.instantToLongSource(
                    (ColumnSource<Instant>) valueSource);
            columnType = valueSource.getType();
        } else if (columnType == Boolean.class) {
            // noinspection unchecked
            valueSource = (ColumnSource<DATA_TYPE>) ReinterpretUtils.booleanToByteSource(
                    (ColumnSource<Boolean>) valueSource);
        }

        try (final ColumnWriter columnWriter = rowGroupWriter.addColumn(
                writeInstructions.getParquetColumnNameFromColumnNameOrDefault(name))) {
            boolean usedDictionary = false;
            if (valueSource.getType() == String.class) {
                usedDictionary = tryEncodeDictionary(writeInstructions,
                        tableRowSet,
                        columnDefinition,
                        columnWriter,
                        valueSource,
                        helper,
                        maxValuesPerPage,
                        maxRowsPerPage,
                        pageCount);
            }

            if (!usedDictionary) {
                encodePlain(writeInstructions,
                        tableRowSet,
                        columnDefinition,
                        columnType,
                        columnWriter,
                        valueSource,
                        helper,
                        computedCache,
                        maxValuesPerPage,
                        maxRowsPerPage,
                        pageCount);
            }
        }
    }

    private static <DATA_TYPE> void encodePlain(@NotNull final ParquetInstructions writeInstructions,
            @NotNull final RowSet tableRowSet,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final Class<DATA_TYPE> columnType,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final ColumnSource<DATA_TYPE> valueSource,
            @NotNull final ColumnWriteHelper writingHelper,
            @NotNull final Map<String, Map<CacheTags, Object>> computedCache,
            final int maxValuesPerPage,
            final int maxRowsPerPage,
            final int pageCount) throws IOException {
        try (final TransferObject<?> transferObject = getDestinationBuffer(computedCache,
                tableRowSet,
                valueSource,
                columnDefinition,
                maxValuesPerPage,
                columnType,
                writeInstructions)) {
            final VectorColumnWriterHelper vectorHelper = writingHelper.isVectorFormat()
                    ? (VectorColumnWriterHelper) writingHelper
                    : null;
            final Statistics<?> statistics = columnWriter.getStats();
            // @formatter:off
            try (final RowSequence.Iterator lengthRowSetIterator = vectorHelper != null
                    ? tableRowSet.getRowSequenceIterator()
                    : null;
                 final ChunkSource.GetContext lengthSourceContext = vectorHelper != null
                         ? vectorHelper.lengthSource.makeGetContext(maxRowsPerPage)
                         : null;
                 final RowSequence.Iterator valueRowSetIterator = vectorHelper != null
                         ? vectorHelper.valueRowSet.getRowSequenceIterator()
                         : tableRowSet.getRowSequenceIterator()) {
                // @formatter:on

                final IntBuffer repeatCount = vectorHelper != null
                        ? IntBuffer.allocate(maxRowsPerPage)
                        : null;
                final IntSupplier lengthPageSizeGetter = vectorHelper != null
                        ? vectorHelper.lengthPageSizeSupplier()
                        : null;
                final IntSupplier valuePageSizeGetter = writingHelper.valuePageSizeSupplier();
                for (int step = 0; step < pageCount; ++step) {
                    final RowSequence rs =
                            valueRowSetIterator.getNextRowSequenceWithLength(valuePageSizeGetter.getAsInt());
                    transferObject.fetchData(rs);
                    if (vectorHelper != null) {
                        final IntChunk<? extends Values> lenChunk = vectorHelper.lengthSource.getChunk(
                                lengthSourceContext,
                                lengthRowSetIterator.getNextRowSequenceWithLength(lengthPageSizeGetter.getAsInt()))
                                .asIntChunk();
                        lenChunk.copyToTypedBuffer(0, repeatCount, 0, lenChunk.size());
                        repeatCount.limit(lenChunk.size());
                        // Write all the fetched vector data into a single Parquet page.
                        // This can lead to out-of-memory errors for variable-width types if the entries are very long.
                        int numValuesBuffered = transferObject.transferAllToBuffer();
                        columnWriter.addVectorPage(transferObject.getBuffer(), repeatCount, numValuesBuffered,
                                statistics);
                        repeatCount.clear();
                    } else {
                        // Split a single page into multiple if we are not able to fit all the entries in one page
                        do {
                            int numValuesBuffered = transferObject.transferOnePageToBuffer();
                            columnWriter.addPage(transferObject.getBuffer(), numValuesBuffered, statistics);
                        } while (transferObject.hasMoreDataToBuffer());
                    }
                }
            }
        }
    }

    private static <DATA_TYPE> boolean tryEncodeDictionary(
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final RowSet tableRowSet,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final ColumnSource<DATA_TYPE> valueSource,
            @NotNull final ColumnWriteHelper writingHelper,
            final int maxValuesPerPage,
            final int maxRowsPerPage,
            final int pageCount) throws IOException {
        // Note: We only support strings as dictionary pages. Knowing that, we can make some assumptions about chunk
        // types and avoid a bunch of lambda and virtual method invocations. If we decide to support more, than
        // these assumptions will need to be revisited.
        Assert.eq(valueSource.getType(), "valueSource.getType()", String.class, "ColumnSource supports dictionary");

        final boolean useDictionaryHint = writeInstructions.useDictionary(columnDefinition.getName());
        final int maxKeys = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionaryKeys();
        final int maxDictSize = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionarySize();
        final VectorColumnWriterHelper vectorHelper = writingHelper.isVectorFormat()
                ? (VectorColumnWriterHelper) writingHelper
                : null;
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
            final IntSupplier valuePageSizeGetter = writingHelper.valuePageSizeSupplier();
            try (final ChunkSource.GetContext context = valueSource.makeGetContext(maxValuesPerPage);
                    final RowSequence.Iterator it = vectorHelper != null
                            ? vectorHelper.valueRowSet.getRowSequenceIterator()
                            : tableRowSet.getRowSequenceIterator()) {
                for (int curPage = 0; curPage < pageCount; curPage++) {
                    boolean pageHasNulls = false;
                    final RowSequence rs = it.getNextRowSequenceWithLength(valuePageSizeGetter.getAsInt());
                    final ObjectChunk<String, ? extends Values> chunk =
                            valueSource.getChunk(context, rs).asObjectChunk();
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
                }
            }

            if (keyCount == 0 && hasNulls) {
                // Reset the stats because we will re-encode these in PLAIN encoding.
                columnWriter.resetStats();
                return false;
            }

            List<IntBuffer> arraySizeBuffers = null;
            if (vectorHelper != null) {
                arraySizeBuffers = new ArrayList<>();
                final IntSupplier lengthPageSizeGetter = vectorHelper.lengthPageSizeSupplier();
                try (final ChunkSource.GetContext context =
                        vectorHelper.lengthSource.makeGetContext(maxRowsPerPage);
                        final RowSequence.Iterator it = tableRowSet.getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        final RowSequence rs = it.getNextRowSequenceWithLength(lengthPageSizeGetter.getAsInt());
                        final IntChunk<? extends Values> chunk =
                                vectorHelper.lengthSource.getChunk(context, rs).asIntChunk();
                        final IntBuffer newBuffer = IntBuffer.allocate(chunk.size());
                        chunk.copyToTypedBuffer(0, newBuffer, 0, chunk.size());
                        newBuffer.limit(chunk.size());
                        arraySizeBuffers.add(newBuffer);
                    }
                }
            }

            columnWriter.addDictionaryPage(encodedKeys, keyCount);
            final Iterator<IntBuffer> arraySizeIt = arraySizeBuffers == null ? null : arraySizeBuffers.iterator();
            // We've already determined min/max statistics while building the dictionary. Now use an integer statistics
            // object to track the number of nulls that will be written.
            Statistics<Integer> tmpStats = new IntStatistics();
            for (int i = 0; i < pageBuffers.size(); ++i) {
                final IntBuffer pageBuffer = pageBuffers.get(i);
                final boolean pageHasNulls = pageBufferHasNull.get(i);
                pageBuffer.flip();
                if (vectorHelper != null) {
                    columnWriter.addVectorPage(pageBuffer, arraySizeIt.next(), pageBuffer.remaining(), tmpStats);
                } else if (pageHasNulls) {
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

    /**
     * Get the number of rows that fit within the current targetPageSize for the specified type.
     *
     * @param columnType the column type
     * @return the number of rows that fit within the target page size.
     */
    private static int getTargetRowsPerPage(@NotNull final Class<?> columnType,
            final int targetPageSize)
            throws IllegalAccessException {
        if (columnType == Boolean.class) {
            return targetPageSize * 8;
        }

        if (columnType == short.class || columnType == char.class || columnType == byte.class) {
            return targetPageSize / Integer.BYTES;
        }

        if (columnType == String.class) {
            // We don't know the length of strings until we read the actual data. Therefore, we take a relaxed estimate
            // here and final calculation is done when writing the data.
            return targetPageSize;
        }

        try {
            final Field bytesCountField = TypeUtils.getBoxedType(columnType).getField("BYTES");
            return targetPageSize / ((Integer) bytesCountField.get(null));
        } catch (NoSuchFieldException e) {
            // We assume the baseline and go from there
            return targetPageSize / 8;
        }
    }

    private static <DATA_TYPE> TransferObject<?> getDestinationBuffer(
            @NotNull final Map<String, Map<CacheTags, Object>> computedCache,
            @NotNull final RowSet tableRowSet,
            @NotNull final ColumnSource<DATA_TYPE> columnSource,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            final int maxValuesPerPage,
            @NotNull final Class<DATA_TYPE> columnType,
            @NotNull final ParquetInstructions instructions) {
        if (int.class.equals(columnType)) {
            int[] array = new int[maxValuesPerPage];
            WritableIntChunk<Values> chunk = WritableIntChunk.writableChunkWrap(array);
            return new IntTransfer(columnSource, chunk, IntBuffer.wrap(array), maxValuesPerPage);
        } else if (long.class.equals(columnType)) {
            long[] array = new long[maxValuesPerPage];
            WritableLongChunk<Values> chunk = WritableLongChunk.writableChunkWrap(array);
            return new LongTransfer(columnSource, chunk, LongBuffer.wrap(array), maxValuesPerPage);
        } else if (double.class.equals(columnType)) {
            double[] array = new double[maxValuesPerPage];
            WritableDoubleChunk<Values> chunk = WritableDoubleChunk.writableChunkWrap(array);
            return new DoubleTransfer(columnSource, chunk, DoubleBuffer.wrap(array), maxValuesPerPage);
        } else if (float.class.equals(columnType)) {
            float[] array = new float[maxValuesPerPage];
            WritableFloatChunk<Values> chunk = WritableFloatChunk.writableChunkWrap(array);
            return new FloatTransfer(columnSource, chunk, FloatBuffer.wrap(array), maxValuesPerPage);
        } else if (Boolean.class.equals(columnType)) {
            byte[] array = new byte[maxValuesPerPage];
            WritableByteChunk<Values> chunk = WritableByteChunk.writableChunkWrap(array);
            return new BooleanTransfer(columnSource, chunk, ByteBuffer.wrap(array), maxValuesPerPage);
        } else if (short.class.equals(columnType)) {
            return new ShortTransfer(columnSource, maxValuesPerPage);
        } else if (char.class.equals(columnType)) {
            return new CharTransfer(columnSource, maxValuesPerPage);
        } else if (byte.class.equals(columnType)) {
            return new ByteTransfer(columnSource, maxValuesPerPage);
        } else if (String.class.equals(columnType)) {
            return new StringTransfer(columnSource, maxValuesPerPage, instructions.getTargetPageSize());
        }

        // If there's an explicit codec, we should disregard the defaults for these CodecLookup#lookup() will properly
        // select the codec assigned by the instructions so we only need to check and redirect once.
        if (!CodecLookup.explicitCodecPresent(instructions.getCodecName(columnDefinition.getName()))) {
            if (BigDecimal.class.equals(columnType)) {
                // noinspection unchecked
                final ColumnSource<BigDecimal> bigDecimalColumnSource = (ColumnSource<BigDecimal>) columnSource;
                final BigDecimalUtils.PrecisionAndScale precisionAndScale = TypeInfos.getPrecisionAndScale(
                        computedCache, columnDefinition.getName(), tableRowSet, () -> bigDecimalColumnSource);
                final ObjectCodec<BigDecimal> codec = new BigDecimalParquetBytesCodec(
                        precisionAndScale.precision, precisionAndScale.scale, -1);
                return new CodecTransfer<>(bigDecimalColumnSource, codec, maxValuesPerPage,
                        instructions.getTargetPageSize());
            } else if (BigInteger.class.equals(columnType)) {
                // noinspection unchecked
                return new CodecTransfer<>((ColumnSource<BigInteger>) columnSource, new BigIntegerParquetBytesCodec(-1),
                        maxValuesPerPage, instructions.getTargetPageSize());
            }
        }

        final ObjectCodec<? super DATA_TYPE> codec = CodecLookup.lookup(columnDefinition, instructions);
        return new CodecTransfer<>(columnSource, codec, maxValuesPerPage, instructions.getTargetPageSize());
    }

    static class PrimitiveTransfer<C extends WritableChunk<Values>, B extends Buffer>
            implements TransferObject<B> {
        private final C chunk;
        private final B buffer;
        private final ColumnSource<?> columnSource;
        private final ChunkSource.FillContext context;
        private boolean hasMoreDataToBuffer;

        /**
         * {@code chunk} and {@code buffer} must be backed by the same array. Assumption verified in the child classes.
         */
        PrimitiveTransfer(ColumnSource<?> columnSource, C chunk, B buffer, int targetSize) {
            this.columnSource = columnSource;
            this.chunk = chunk;
            this.buffer = buffer;
            context = columnSource.makeFillContext(targetSize);
        }

        @Override
        public boolean hasMoreDataToBuffer() {
            return hasMoreDataToBuffer;
        }

        @Override
        public int transferAllToBuffer() {
            return transferOnePageToBuffer();
        }

        @Override
        public int transferOnePageToBuffer() {
            if (!hasMoreDataToBuffer()) {
                return 0;
            }
            // Assuming that buffer and chunk are backed by the same array.
            buffer.position(0);
            buffer.limit(chunk.size());
            hasMoreDataToBuffer = false;
            return buffer.limit();
        }

        @Override
        public B getBuffer() {
            return buffer;
        }

        @Override
        public void fetchData(RowSequence rs) {
            columnSource.fillChunk(context, chunk, rs);
            hasMoreDataToBuffer = true;
        }

        @Override
        public void close() {
            context.close();
        }
    }

    final static class IntTransfer extends PrimitiveTransfer<WritableIntChunk<Values>, IntBuffer> {
        /**
         * Check docs in {@link PrimitiveTransfer} for more details.
         */
        IntTransfer(ColumnSource<?> columnSource, WritableIntChunk<Values> chunk, IntBuffer buffer, int targetSize) {
            super(columnSource, chunk, buffer, targetSize);
            assert (Arrays.equals(chunk.array(), buffer.array()));
        }
    }

    final static class LongTransfer extends PrimitiveTransfer<WritableLongChunk<Values>, LongBuffer> {
        /**
         * Check docs in {@link PrimitiveTransfer} for more details.
         */
        LongTransfer(ColumnSource<?> columnSource, WritableLongChunk<Values> chunk, LongBuffer buffer, int targetSize) {
            super(columnSource, chunk, buffer, targetSize);
            assert (Arrays.equals(chunk.array(), buffer.array()));
        }
    }

    final static class DoubleTransfer extends PrimitiveTransfer<WritableDoubleChunk<Values>, DoubleBuffer> {
        /**
         * Check docs in {@link PrimitiveTransfer} for more details.
         */
        DoubleTransfer(ColumnSource<?> columnSource, WritableDoubleChunk<Values> chunk, DoubleBuffer buffer,
                int targetSize) {
            super(columnSource, chunk, buffer, targetSize);
            assert (Arrays.equals(chunk.array(), buffer.array()));
        }
    }

    final static class FloatTransfer extends PrimitiveTransfer<WritableFloatChunk<Values>, FloatBuffer> {
        /**
         * Check docs in {@link PrimitiveTransfer} for more details.
         */
        FloatTransfer(ColumnSource<?> columnSource, WritableFloatChunk<Values> chunk, FloatBuffer buffer,
                int targetSize) {
            super(columnSource, chunk, buffer, targetSize);
            assert (Arrays.equals(chunk.array(), buffer.array()));
        }
    }

    final static class BooleanTransfer extends PrimitiveTransfer<WritableByteChunk<Values>, ByteBuffer> {
        /**
         * Check docs in {@link PrimitiveTransfer} for more details.
         */
        BooleanTransfer(ColumnSource<?> columnSource, WritableByteChunk<Values> chunk, ByteBuffer buffer,
                int targetSize) {
            super(columnSource, chunk, buffer, targetSize);
            assert (Arrays.equals(chunk.array(), buffer.array()));
        }
    }

    /**
     * Used as a base class of transfer objects for types like shorts which are castable to Ints without losing any
     * precision and are therefore stored using IntBuffer.
     */
    abstract static class IntCastablePrimitiveTransfer<T extends ChunkBase<Values>>
            implements TransferObject<IntBuffer> {
        protected T chunk;
        protected final IntBuffer buffer;
        private final ColumnSource<?> columnSource;
        private final ChunkSource.GetContext context;

        IntCastablePrimitiveTransfer(ColumnSource<?> columnSource, int targetSize) {
            this.columnSource = columnSource;
            this.buffer = IntBuffer.allocate(targetSize);
            context = columnSource.makeGetContext(targetSize);
        }

        @Override
        final public boolean hasMoreDataToBuffer() {
            return (chunk != null);
        }

        @Override
        final public int transferAllToBuffer() {
            return transferOnePageToBuffer();
        }

        @Override
        final public int transferOnePageToBuffer() {
            if (!hasMoreDataToBuffer()) {
                return 0;
            }
            buffer.clear();
            // Assuming that all the fetched data will fit in one page. This is because page count is accurately
            // calculated for non variable-width types. Check ParquetTableWriter.getTargetRowsPerPage for more details.
            copyAllFromChunkToBuffer();
            buffer.flip();
            int ret = chunk.size();
            chunk = null;
            return ret;
        }

        /**
         * Helper method to copy all data from {@code this.chunk} to {@code this.buffer}. The buffer should be cleared
         * before calling this method and is positioned for a {@link Buffer#flip()} after the call.
         */
        abstract void copyAllFromChunkToBuffer();

        @Override
        final public IntBuffer getBuffer() {
            return buffer;
        }

        @Override
        final public void fetchData(RowSequence rs) {
            // noinspection unchecked
            chunk = (T) columnSource.getChunk(context, rs);
        }

        @Override
        final public void close() {
            context.close();
        }
    }

    final static class ShortTransfer extends IntCastablePrimitiveTransfer<ShortChunk<Values>> {
        ShortTransfer(ColumnSource<?> columnSource, int targetSize) {
            super(columnSource, targetSize);
        }

        @Override
        void copyAllFromChunkToBuffer() {
            for (int chunkIdx = 0; chunkIdx < chunk.size(); chunkIdx++) {
                buffer.put(chunk.get(chunkIdx));
            }
        }
    }

    final static class CharTransfer extends IntCastablePrimitiveTransfer<CharChunk<Values>> {
        CharTransfer(ColumnSource<?> columnSource, int targetSize) {
            super(columnSource, targetSize);
        }

        @Override
        void copyAllFromChunkToBuffer() {
            for (int chunkIdx = 0; chunkIdx < chunk.size(); chunkIdx++) {
                buffer.put(chunk.get(chunkIdx));
            }
        }
    }

    final static class ByteTransfer extends IntCastablePrimitiveTransfer<ByteChunk<Values>> {
        ByteTransfer(ColumnSource<?> columnSource, int targetSize) {
            super(columnSource, targetSize);
        }

        @Override
        void copyAllFromChunkToBuffer() {
            for (int chunkIdx = 0; chunkIdx < chunk.size(); chunkIdx++) {
                buffer.put(chunk.get(chunkIdx));
            }
        }
    }

    /**
     * Used as a base class of transfer objects for types like strings or big integers that need specialized encoding,
     * and thus we need to enforce page size limits while writing.
     */
    abstract static class EncodedTransfer<T> implements TransferObject<Binary[]> {
        private final ChunkSource.GetContext context;
        private ObjectChunk<T, Values> chunk;
        private Binary[] buffer;
        /**
         * Number of objects buffered
         */
        private int bufferCount;

        private final ColumnSource<?> columnSource;

        /**
         * The target size of data to be stored in a single page. This is not a strictly enforced "maximum" page size.
         */
        private final int targetPageSize;

        /**
         * Index of next object from the chunk to be buffered
         */
        private int currentChunkIdx;

        EncodedTransfer(ColumnSource<?> columnSource, int maxValuesPerPage, int targetPageSize) {
            this.columnSource = columnSource;
            this.buffer = new Binary[maxValuesPerPage];
            context = this.columnSource.makeGetContext(maxValuesPerPage);
            this.targetPageSize = targetPageSize;
            bufferCount = 0;
        }

        @Override
        final public boolean hasMoreDataToBuffer() {
            return ((chunk != null) && (currentChunkIdx < chunk.size()));
        }

        @Override
        final public int transferAllToBuffer() {
            if (!hasMoreDataToBuffer()) {
                return 0;
            }
            // The call to transferAllToBuffer() should not be interleaved with transferOnePageToBuffer().
            assert (currentChunkIdx == 0 && bufferCount == 0);
            int chunkSize = chunk.size();
            while (currentChunkIdx < chunkSize) {
                final T value = chunk.get(currentChunkIdx++);
                buffer[bufferCount++] = value == null ? null : encodeToBinary(value);
            }
            return bufferCount;
        }

        @Override
        final public int transferOnePageToBuffer() {
            if (!hasMoreDataToBuffer()) {
                return 0;
            }
            if (bufferCount != 0) {
                // Clear any old buffered data
                Arrays.fill(buffer, 0, bufferCount, null);
                bufferCount = 0;
            }
            int size = 0;
            while (currentChunkIdx < chunk.size()) {
                final T value = chunk.get(currentChunkIdx++);
                if (value == null) {
                    buffer[bufferCount++] = null;
                    continue;
                }
                Binary binaryVal = encodeToBinary(value);
                buffer[bufferCount++] = binaryVal;
                size += binaryVal.length();
                if (size >= targetPageSize) {
                    break;
                }
            }
            if (currentChunkIdx == chunk.size()) {
                chunk = null;
            }
            return bufferCount;
        }

        abstract Binary encodeToBinary(T value);

        @Override
        final public Binary[] getBuffer() {
            return buffer;
        }

        @Override
        final public void fetchData(RowSequence rs) {
            // noinspection unchecked
            chunk = (ObjectChunk<T, Values>) columnSource.getChunk(context, rs);
            currentChunkIdx = 0;
        }

        @Override
        final public void close() {
            context.close();
        }
    }

    final static class StringTransfer extends EncodedTransfer<String> {
        StringTransfer(ColumnSource<?> columnSource, int maxValuesPerPage, int targetPageSize) {
            super(columnSource, maxValuesPerPage, targetPageSize);
        }

        @Override
        Binary encodeToBinary(String value) {
            return Binary.fromString(value);
        }
    }

    final static class CodecTransfer<T> extends EncodedTransfer<T> {
        private final ObjectCodec<? super T> codec;

        CodecTransfer(ColumnSource<?> columnSource, ObjectCodec<? super T> codec, int maxValuesPerPage,
                int targetPageSize) {
            super(columnSource, maxValuesPerPage, targetPageSize);
            this.codec = codec;
        }

        @Override
        Binary encodeToBinary(T value) {
            return Binary.fromConstantByteArray(codec.encode(value));
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

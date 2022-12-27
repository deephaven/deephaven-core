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
import io.deephaven.time.DateTime;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.LongSupplier;

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
        void propagateChunkData();

        B getBuffer();

        int rowCount();

        void fetchData(RowSequence rs);
    }

    private static String minusParquetSuffix(@NotNull final String s) {
        if (s.endsWith(PARQUET_FILE_EXTENSION)) {
            return s.substring(0, s.length() - PARQUET_FILE_EXTENSION.length());
        }
        return s;
    }

    public static Function<String, String> defaultGroupingFileName(@NotNull final String path) {
        final String prefix = minusParquetSuffix(path);
        return columnName -> prefix + "_" + columnName + "_grouping.parquet";
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t The table to write
     * @param path The destination path
     * @param incomingMeta A map of metadata values to be stores in the file footer
     * @param groupingPathFactory a factory to construct paths for grouping tables.
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping
     *        info)
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to
     *         unsupported types)
     * @throws IOException For file writing related errors
     */
    public static void write(
            @NotNull final Table t,
            @NotNull final String path,
            @NotNull final Map<String, String> incomingMeta,
            @NotNull final Function<String, String> groupingPathFactory,
            @NotNull final String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, t.getDefinition(), ParquetInstructions.EMPTY, path, incomingMeta, groupingPathFactory,
                groupingColumns);
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t the table to write
     * @param path the destination path
     * @param incomingMeta any metadata to include in the parquet metadata
     * @param groupingColumns the grouping columns (if any)
     */
    public static void write(@NotNull final Table t,
            @NotNull final String path,
            @NotNull final Map<String, String> incomingMeta,
            @NotNull final String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, path, incomingMeta, defaultGroupingFileName(path), groupingColumns);
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t The table to write
     * @param definition Table definition
     * @param writeInstructions Write instructions for customizations while writing
     * @param destPathName The destination path
     * @param incomingMeta A map of metadata values to be stores in the file footer
     * @param groupingPathFactory a factory for constructing paths for grouping tables
     * @param groupingColumns List of columns the tables are grouped by (the write operation will store the grouping
     *        info)
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
            @NotNull final Function<String, String> groupingPathFactory,
            @NotNull final String... groupingColumns) throws SchemaMappingException, IOException {
        final TableInfo.Builder tableInfoBuilder = TableInfo.builder();
        ArrayList<String> cleanupPaths = null;
        try {
            if (groupingColumns.length > 0) {
                cleanupPaths = new ArrayList<>(groupingColumns.length);
                final Table[] auxiliaryTables = Arrays.stream(groupingColumns)
                        .map(columnName -> groupingAsTable(t, columnName))
                        .toArray(Table[]::new);

                final Path destDirPath = Paths.get(destPathName).getParent();
                for (int gci = 0; gci < auxiliaryTables.length; ++gci) {
                    final String parquetColumnName =
                            writeInstructions.getParquetColumnNameFromColumnNameOrDefault(groupingColumns[gci]);
                    final String groupingPath = groupingPathFactory.apply(parquetColumnName);
                    cleanupPaths.add(groupingPath);
                    tableInfoBuilder.addGroupingColumns(GroupingColumnInfo.of(parquetColumnName,
                            destDirPath.relativize(Paths.get(groupingPath)).toString()));
                    write(auxiliaryTables[gci], auxiliaryTables[gci].getDefinition(), writeInstructions, groupingPath,
                            Collections.emptyMap());
                }
            }
            write(t, definition, writeInstructions, destPathName, incomingMeta, tableInfoBuilder);
        } catch (Exception e) {
            if (cleanupPaths != null) {
                for (final String cleanupPath : cleanupPaths) {
                    try {
                        // noinspection ResultOfMethodCallIgnored
                        new File(cleanupPath).delete();
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
     * @param t the table to write
     * @param definition the writable table definition
     * @param writeInstructions the parquet instructions for writing
     * @param path the path to write to
     * @param incomingMeta any metadata to include ih the parquet metadata
     * @param groupingColumns the grouping columns (if any)
     */
    public static void write(
            @NotNull final Table t,
            @NotNull final TableDefinition definition,
            @NotNull final ParquetInstructions writeInstructions,
            @NotNull final String path,
            @NotNull final Map<String, String> incomingMeta,
            @NotNull final String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, definition, writeInstructions, path, incomingMeta, defaultGroupingFileName(path), groupingColumns);
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
                    updateViewColumnsTransform.add(FormulaColumn.createFormulaColumn(colName, colName + ".values()"));
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
            transformed =
                    transformed.view(viewColumnsTransform.toArray((SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY)));
        }

        if (!updateViewColumnsTransform.isEmpty()) {
            transformed = transformed
                    .updateView(updateViewColumnsTransform.toArray(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY));
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

    @VisibleForTesting
    static <DATA_TYPE> void writeColumnSource(
            @NotNull final Map<String, Map<CacheTags, Object>> computedCache,
            @NotNull final TrackingRowSet tableRowSet,
            @NotNull final RowGroupWriter rowGroupWriter,
            @NotNull final String name,
            @NotNull final ColumnSource<DATA_TYPE> columnSourceIn,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final ParquetInstructions writeInstructions) throws IllegalAccessException, IOException {
        RowSet rowSet = tableRowSet;
        ColumnSource<DATA_TYPE> columnSource = columnSourceIn;
        ColumnSource<?> lengthSource = null;
        LongSupplier rowStepGetter;
        LongSupplier valuesStepGetter;

        int maxValuesPerPage = 0;
        int maxOriginalRowsPerPage = 0;
        int pageCount;
        if (columnSource.getComponentType() != null
                && !CodecLookup.explicitCodecPresent(writeInstructions.getCodecName(columnDefinition.getName()))
                && !CodecLookup.codecRequired(columnDefinition)) {
            final int targetRowsPerPage = getTargetRowsPerPage(columnSource.getComponentType(),
                    writeInstructions.getTargetPageSize());
            final HashMap<String, ColumnSource<?>> columns = new HashMap<>();
            columns.put("array", columnSource);
            final Table lengthsTable = new QueryTable(tableRowSet, columns);
            lengthSource = lengthsTable
                    .view("len= ((Object)array) == null ? null : (int)array."
                            + (Vector.class.isAssignableFrom(columnSource.getType()) ? "size()" : "length"))
                    .getColumnSource("len");

            // These two lists are parallel, where each element represents a single page. The rawItemCountPerPage
            // contains the number of items (the sum of the array sizes for each containing row) contained within the
            // page.
            final TIntArrayList rawItemCountPerPage = new TIntArrayList();

            // The originalRowsPerPage contains the number of arrays (rows) from the original table contained within
            // the page.
            final TIntArrayList originalRowsPerPage = new TIntArrayList();

            // This is the count of items contained in all arrays from the original table as we process.
            int totalItemsInPage = 0;

            // This is the count of rows in the original table as we process them
            int originalRowsInPage = 0;
            try (final ChunkSource.GetContext context = lengthSource.makeGetContext(LOCAL_CHUNK_SIZE);
                    final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(LOCAL_CHUNK_SIZE);
                    final IntChunk<? extends Values> lengthChunk = lengthSource.getChunk(context, rs).asIntChunk();
                    for (int chunkPos = 0; chunkPos < lengthChunk.size(); chunkPos++) {
                        final int curLength = lengthChunk.get(chunkPos);
                        if (curLength != NULL_INT) {
                            // If this array puts us past the target number of items within a page then we'll record the
                            // current values into the page lists above and restart our counts.
                            if (((totalItemsInPage + curLength > targetRowsPerPage) ||
                                    (originalRowsInPage + 1 > targetRowsPerPage)) &&
                                    (totalItemsInPage > 0 || originalRowsInPage > 0)) {
                                // Record the current item count and original row count into the parallel page arrays.
                                rawItemCountPerPage.add(totalItemsInPage);
                                originalRowsPerPage.add(originalRowsInPage);
                                maxValuesPerPage = Math.max(totalItemsInPage, maxValuesPerPage);
                                maxOriginalRowsPerPage = Math.max(originalRowsInPage, maxOriginalRowsPerPage);

                                // Reset the counts to compute these values for the next page.
                                originalRowsInPage = 0;
                                totalItemsInPage = 0;
                            }
                            totalItemsInPage += curLength;
                        }
                        originalRowsInPage++;
                    }
                }
            }

            // If there are any leftover, accumulate the last page.
            if (originalRowsInPage > 0) {
                maxValuesPerPage = Math.max(totalItemsInPage, maxValuesPerPage);
                maxOriginalRowsPerPage = Math.max(originalRowsInPage, maxOriginalRowsPerPage);
                rawItemCountPerPage.add(totalItemsInPage);
                originalRowsPerPage.add(originalRowsInPage);
            }

            rowStepGetter = originalRowsPerPage.iterator()::next;
            valuesStepGetter = rawItemCountPerPage.iterator()::next;

            pageCount = rawItemCountPerPage.size();
            final Table ungroupedArrays = lengthsTable.ungroup("array");
            rowSet = ungroupedArrays.getRowSet();
            columnSource = ungroupedArrays.getColumnSource("array");
        } else {
            final int finalTargetSize = getTargetRowsPerPage(columnSource.getType(),
                    writeInstructions.getTargetPageSize());
            rowStepGetter = valuesStepGetter = () -> finalTargetSize;
            maxValuesPerPage = maxOriginalRowsPerPage = (int) Math.min(rowSet.size(), finalTargetSize);
            pageCount = (int) (rowSet.size() / finalTargetSize + ((rowSet.size() % finalTargetSize) == 0 ? 0 : 1));
        }

        Class<DATA_TYPE> columnType = columnSource.getType();
        if (columnType == DateTime.class) {
            // noinspection unchecked
            columnSource = (ColumnSource<DATA_TYPE>) ReinterpretUtils.dateTimeToLongSource(columnSource);
            columnType = columnSource.getType();
        } else if (columnType == Boolean.class) {
            // noinspection unchecked
            columnSource = (ColumnSource<DATA_TYPE>) ReinterpretUtils.booleanToByteSource(columnSource);
        }

        try (final ColumnWriter columnWriter = rowGroupWriter.addColumn(
                writeInstructions.getParquetColumnNameFromColumnNameOrDefault(name))) {
            boolean usedDictionary = false;
            if (columnSource.getType() == String.class) {
                usedDictionary = tryEncodeDictionary(writeInstructions,
                        tableRowSet,
                        rowSet,
                        columnDefinition,
                        columnWriter,
                        columnSource,
                        lengthSource,
                        rowStepGetter,
                        valuesStepGetter,
                        maxValuesPerPage,
                        maxOriginalRowsPerPage,
                        pageCount);
            }

            if (!usedDictionary) {
                encodePlain(writeInstructions,
                        tableRowSet,
                        rowSet,
                        columnDefinition,
                        columnType,
                        columnWriter,
                        columnSource,
                        lengthSource,
                        rowStepGetter,
                        valuesStepGetter,
                        computedCache,
                        maxValuesPerPage,
                        maxOriginalRowsPerPage,
                        pageCount);
            }
        }
    }

    private static <DATA_TYPE> void encodePlain(@NotNull final ParquetInstructions writeInstructions,
            @NotNull final RowSet originalRowSet,
            @NotNull final RowSet dataRowSet,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final Class<DATA_TYPE> columnType,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final ColumnSource<DATA_TYPE> dataSource,
            @Nullable final ColumnSource<?> lengthSource,
            @NotNull final LongSupplier rowStepGetter,
            @NotNull final LongSupplier valuesStepGetter,
            @NotNull final Map<String, Map<CacheTags, Object>> computedCache,
            final int maxValuesPerPage,
            final int maxOriginalRowsPerPage,
            final int pageCount) throws IOException {
        try (final TransferObject<?> transferObject = getDestinationBuffer(computedCache,
                originalRowSet,
                dataSource,
                columnDefinition,
                maxValuesPerPage,
                columnType,
                writeInstructions)) {
            final boolean supportNulls = supportNulls(columnType);
            final Object bufferToWrite = transferObject.getBuffer();
            try (final RowSequence.Iterator lengthIndexIt =
                    lengthSource != null ? originalRowSet.getRowSequenceIterator() : null;
                    final ChunkSource.GetContext lengthSourceContext =
                            lengthSource != null ? lengthSource.makeGetContext(maxOriginalRowsPerPage) : null;
                    final RowSequence.Iterator it = dataRowSet.getRowSequenceIterator()) {

                final IntBuffer repeatCount = lengthSource != null ? IntBuffer.allocate(maxOriginalRowsPerPage) : null;
                for (int step = 0; step < pageCount; ++step) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(valuesStepGetter.getAsLong());
                    transferObject.fetchData(rs);
                    transferObject.propagateChunkData();
                    if (lengthIndexIt != null) {
                        final IntChunk<? extends Values> lenChunk = lengthSource.getChunk(
                                lengthSourceContext,
                                lengthIndexIt.getNextRowSequenceWithLength(rowStepGetter.getAsLong())).asIntChunk();
                        lenChunk.copyToTypedBuffer(0, repeatCount, 0, lenChunk.size());
                        repeatCount.limit(lenChunk.size());
                        columnWriter.addVectorPage(bufferToWrite, repeatCount, transferObject.rowCount());
                        repeatCount.clear();
                    } else if (supportNulls) {
                        columnWriter.addPage(bufferToWrite, transferObject.rowCount());
                    } else {
                        columnWriter.addPageNoNulls(bufferToWrite, transferObject.rowCount());
                    }
                }
            }
        }
    }

    private static <DATA_TYPE> boolean tryEncodeDictionary(@NotNull final ParquetInstructions writeInstructions,
            @NotNull final RowSet originalRowSet,
            @NotNull final RowSet dataRowSet,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final ColumnWriter columnWriter,
            @NotNull final ColumnSource<DATA_TYPE> dataSource,
            @Nullable final ColumnSource<?> lengthSource,
            @NotNull final LongSupplier rowStepGetter,
            @NotNull final LongSupplier valuesStepGetter,
            final int maxRowsPerPage,
            final int maxOriginalRowsPerPage,
            final int pageCount) throws IOException {
        // Note: We only support strings as dictionary pages. Knowing that, we can make some assumptions about chunk
        // types and avoid a bunch of lambda and virtual method invocations. If we decide to support more, than
        // these assumptions will need to be revisited.
        Assert.eq(dataSource.getType(), "dataSource.getType()", String.class, "ColumnSource supports dictionary");

        final boolean useDictionaryHint = writeInstructions.useDictionary(columnDefinition.getName());
        final int maxKeys = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionaryKeys();
        try {
            final List<IntBuffer> pageBuffers = new ArrayList<>();
            Binary[] encodedKeys = new Binary[Math.min(INITIAL_DICTIONARY_SIZE, maxKeys)];

            final TObjectIntHashMap<String> keyToPos =
                    new TObjectIntHashMap<>(Constants.DEFAULT_CAPACITY,
                            Constants.DEFAULT_LOAD_FACTOR,
                            QueryConstants.NULL_INT);
            int keyCount = 0;
            boolean hasNulls = false;
            try (final ChunkSource.GetContext context = dataSource.makeGetContext(maxRowsPerPage);
                    final RowSequence.Iterator it = dataRowSet.getRowSequenceIterator()) {
                for (int curPage = 0; curPage < pageCount; curPage++) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(valuesStepGetter.getAsLong());
                    final ObjectChunk<String, ? extends Values> chunk =
                            dataSource.getChunk(context, rs).asObjectChunk();
                    final IntBuffer posInDictionary = IntBuffer.allocate(rs.intSize());
                    for (int vi = 0; vi < chunk.size(); ++vi) {
                        final String key = chunk.get(vi);
                        int dictionaryPos = keyToPos.get(key);
                        if (dictionaryPos == keyToPos.getNoEntryValue()) {
                            if (key == null) {
                                hasNulls = true;
                            } else {
                                if (keyCount == encodedKeys.length) {
                                    if (keyCount >= maxKeys) {
                                        throw new DictionarySizeExceededException(
                                                "Dictionary maximum size exceeded for " + columnDefinition.getName());
                                    }

                                    encodedKeys = Arrays.copyOf(encodedKeys, (int) Math.min(keyCount * 2L, maxKeys));
                                }
                                encodedKeys[keyCount] = Binary.fromString(key);
                                dictionaryPos = keyCount;
                                keyCount++;
                            }
                            keyToPos.put(key, dictionaryPos);
                        }
                        posInDictionary.put(dictionaryPos);
                    }
                    pageBuffers.add(posInDictionary);
                }
            }

            List<IntBuffer> arraySizeBuffers = null;
            if (lengthSource != null) {
                arraySizeBuffers = new ArrayList<>();
                try (final ChunkSource.GetContext context = lengthSource.makeGetContext(maxOriginalRowsPerPage);
                        final RowSequence.Iterator it = originalRowSet.getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        final RowSequence rs = it.getNextRowSequenceWithLength(rowStepGetter.getAsLong());
                        final IntChunk<? extends Values> chunk = lengthSource.getChunk(context, rs).asIntChunk();
                        final IntBuffer newBuffer = IntBuffer.allocate(chunk.size());
                        chunk.copyToTypedBuffer(0, newBuffer, 0, chunk.size());
                        newBuffer.limit(chunk.size());
                        arraySizeBuffers.add(newBuffer);
                    }
                }
            }

            if (keyCount == 0 && hasNulls) {
                return false;
            }

            columnWriter.addDictionaryPage(encodedKeys, keyCount);
            final Iterator<IntBuffer> arraySizeIt = arraySizeBuffers == null ? null : arraySizeBuffers.iterator();
            for (final IntBuffer pageBuffer : pageBuffers) {
                pageBuffer.flip();
                if (lengthSource != null) {
                    columnWriter.addVectorPage(pageBuffer, arraySizeIt.next(), pageBuffer.remaining());
                } else if (hasNulls) {
                    columnWriter.addPage(pageBuffer, pageBuffer.remaining());
                } else {
                    columnWriter.addPageNoNulls(pageBuffer, pageBuffer.remaining());
                }
            }
            return true;
        } catch (final DictionarySizeExceededException ignored) {
            return false;
        }
    }

    private static boolean supportNulls(@NotNull final Class<?> columnType) {
        return !columnType.isPrimitive();
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
            return targetPageSize / Integer.BYTES;
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
            return new PrimitiveTransfer<>(columnSource, chunk, IntBuffer.wrap(array), maxValuesPerPage);
        } else if (long.class.equals(columnType)) {
            long[] array = new long[maxValuesPerPage];
            WritableLongChunk<Values> chunk = WritableLongChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, LongBuffer.wrap(array), maxValuesPerPage);
        } else if (double.class.equals(columnType)) {
            double[] array = new double[maxValuesPerPage];
            WritableDoubleChunk<Values> chunk = WritableDoubleChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, DoubleBuffer.wrap(array), maxValuesPerPage);
        } else if (float.class.equals(columnType)) {
            float[] array = new float[maxValuesPerPage];
            WritableFloatChunk<Values> chunk = WritableFloatChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, FloatBuffer.wrap(array), maxValuesPerPage);
        } else if (Boolean.class.equals(columnType)) {
            byte[] array = new byte[maxValuesPerPage];
            WritableByteChunk<Values> chunk = WritableByteChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, ByteBuffer.wrap(array), maxValuesPerPage);
        } else if (short.class.equals(columnType)) {
            return new ShortTransfer(columnSource, maxValuesPerPage);
        } else if (char.class.equals(columnType)) {
            return new CharTransfer(columnSource, maxValuesPerPage);
        } else if (byte.class.equals(columnType)) {
            return new ByteTransfer(columnSource, maxValuesPerPage);
        } else if (String.class.equals(columnType)) {
            return new StringTransfer(columnSource, maxValuesPerPage);
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
                return new CodecTransfer<>(bigDecimalColumnSource, codec, maxValuesPerPage);
            } else if (BigInteger.class.equals(columnType)) {
                return new CodecTransfer<>((ColumnSource<BigInteger>) columnSource, new BigIntegerParquetBytesCodec(-1),
                        maxValuesPerPage);
            }
        }

        final ObjectCodec<? super DATA_TYPE> codec = CodecLookup.lookup(columnDefinition, instructions);
        return new CodecTransfer<>(columnSource, codec, maxValuesPerPage);
    }

    static class PrimitiveTransfer<C extends WritableChunk<Values>, B extends Buffer> implements TransferObject<B> {
        private final C chunk;
        private final B buffer;
        private final ColumnSource<?> columnSource;
        private final ChunkSource.FillContext context;

        PrimitiveTransfer(ColumnSource<?> columnSource, C chunk, B buffer, int targetSize) {
            this.columnSource = columnSource;
            this.chunk = chunk;
            this.buffer = buffer;
            context = columnSource.makeFillContext(targetSize);
        }

        @Override
        public void propagateChunkData() {
            buffer.position(0);
            buffer.limit(chunk.size());
        }

        @Override
        public B getBuffer() {
            return buffer;
        }

        @Override
        public int rowCount() {
            return chunk.size();
        }


        @Override
        public void fetchData(RowSequence rs) {
            columnSource.fillChunk(context, chunk, rs);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class ShortTransfer implements TransferObject<IntBuffer> {

        private ShortChunk<Values> chunk;
        private final IntBuffer buffer;
        private final ColumnSource<?> columnSource;
        private final ChunkSource.GetContext context;

        ShortTransfer(ColumnSource<?> columnSource, int targetSize) {

            this.columnSource = columnSource;
            this.buffer = IntBuffer.allocate(targetSize);
            context = columnSource.makeGetContext(targetSize);
        }


        @Override
        public void propagateChunkData() {
            buffer.clear();
            for (int i = 0; i < chunk.size(); i++) {
                buffer.put(chunk.get(i));
            }
            buffer.flip();
        }

        @Override
        public IntBuffer getBuffer() {
            return buffer;
        }

        @Override
        public int rowCount() {
            return chunk.size();
        }

        @Override
        public void fetchData(RowSequence rs) {
            // noinspection unchecked
            chunk = (ShortChunk<Values>) columnSource.getChunk(context, rs);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class CharTransfer implements TransferObject<IntBuffer> {

        private final ColumnSource<?> columnSource;
        private final ChunkSource.GetContext context;
        private CharChunk<Values> chunk;
        private final IntBuffer buffer;

        CharTransfer(ColumnSource<?> columnSource, int targetSize) {
            this.columnSource = columnSource;
            this.buffer = IntBuffer.allocate(targetSize);
            context = this.columnSource.makeGetContext(targetSize);
        }

        @Override
        public void propagateChunkData() {
            buffer.clear();
            for (int i = 0; i < chunk.size(); i++) {
                buffer.put(chunk.get(i));
            }
            buffer.flip();
        }

        @Override
        public IntBuffer getBuffer() {
            return buffer;
        }

        @Override
        public int rowCount() {
            return chunk.size();
        }

        @Override
        public void fetchData(RowSequence rs) {
            // noinspection unchecked
            chunk = (CharChunk<Values>) columnSource.getChunk(context, rs);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class ByteTransfer implements TransferObject<IntBuffer> {

        private ByteChunk<Values> chunk;
        private final IntBuffer buffer;
        private final ColumnSource<?> columnSource;
        private final ChunkSource.GetContext context;

        ByteTransfer(ColumnSource<?> columnSource, int targetSize) {
            this.columnSource = columnSource;
            this.buffer = IntBuffer.allocate(targetSize);
            context = this.columnSource.makeGetContext(targetSize);
        }

        @Override
        public void propagateChunkData() {
            buffer.clear();
            for (int i = 0; i < chunk.size(); i++) {
                buffer.put(chunk.get(i));
            }
            buffer.flip();
        }

        @Override
        public IntBuffer getBuffer() {
            return buffer;
        }

        @Override
        public int rowCount() {
            return chunk.size();
        }

        @Override
        public void fetchData(RowSequence rs) {
            // noinspection unchecked
            chunk = (ByteChunk<Values>) columnSource.getChunk(context, rs);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class StringTransfer implements TransferObject<Binary[]> {

        private final ChunkSource.GetContext context;
        private ObjectChunk<String, Values> chunk;
        private final Binary[] buffer;
        private final ColumnSource<?> columnSource;


        StringTransfer(ColumnSource<?> columnSource, int targetSize) {
            this.columnSource = columnSource;
            this.buffer = new Binary[targetSize];
            context = this.columnSource.makeGetContext(targetSize);
        }

        @Override
        public void propagateChunkData() {
            for (int i = 0; i < chunk.size(); i++) {
                String value = chunk.get(i);
                buffer[i] = value == null ? null : Binary.fromString(value);
            }
        }

        @Override
        public Binary[] getBuffer() {
            return buffer;
        }

        @Override
        public int rowCount() {
            return chunk.size();
        }

        @Override
        public void fetchData(RowSequence rs) {
            // noinspection unchecked
            chunk = (ObjectChunk<String, Values>) columnSource.getChunk(context, rs);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class CodecTransfer<T> implements TransferObject<Binary[]> {

        private final ChunkSource.GetContext context;
        private final ObjectCodec<? super T> codec;
        private ObjectChunk<T, Values> chunk;
        private final Binary[] buffer;
        private final ColumnSource<T> columnSource;


        CodecTransfer(ColumnSource<T> columnSource, ObjectCodec<? super T> codec, int targetSize) {
            this.columnSource = columnSource;
            this.buffer = new Binary[targetSize];
            context = this.columnSource.makeGetContext(targetSize);
            this.codec = codec;
        }

        @Override
        public void propagateChunkData() {
            for (int i = 0; i < chunk.size(); i++) {
                T value = chunk.get(i);
                buffer[i] = value == null ? null : Binary.fromConstantByteArray(codec.encode(value));
            }
        }

        @Override
        public Binary[] getBuffer() {
            return buffer;
        }

        @Override
        public int rowCount() {
            return chunk.size();
        }

        @Override
        public void fetchData(RowSequence rs) {
            // noinspection unchecked
            chunk = (ObjectChunk<T, Values>) columnSource.getChunk(context, rs);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    private static Table groupingAsTable(Table tableToSave, String columnName) {
        final QueryTable coalesced = (QueryTable) tableToSave.coalesce();
        final Table tableToGroup = (coalesced.isRefreshing() ? (QueryTable) coalesced.silent() : coalesced)
                .withAttributes(Map.of(Table.STREAM_TABLE_ATTRIBUTE, true)); // We want persistent first/last-by
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

package io.deephaven.db.v2.parquet;

import io.deephaven.db.tables.*;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.locations.parquet.local.TrackedSeekableChannelsProvider;
import io.deephaven.db.v2.parquet.metadata.CodecInfo;
import io.deephaven.db.v2.parquet.metadata.ColumnTypeInfo;
import io.deephaven.db.v2.parquet.metadata.GroupingColumnInfo;
import io.deephaven.db.v2.parquet.metadata.TableInfo;
import io.deephaven.db.v2.select.FormulaColumn;
import io.deephaven.db.v2.select.NullSelectColumn;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.SourceColumn;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.parquet.ColumnWriter;
import io.deephaven.parquet.ParquetFileWriter;
import io.deephaven.parquet.RowGroupWriter;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * API for writing DH tables in parquet format
 */
public class ParquetTableWriter {

    private static final int PAGE_SIZE = 1 << 20;
    private static final int INITIAL_DICTIONARY_SIZE = 1 << 8;

    public static final String METADATA_KEY = "deephaven";

    private static final int LOCAL_CHUNK_SIZE = 1024;

    public static final String BEGIN_POS = "dh_begin_pos";
    public static final String END_POS = "dh_end_pos";
    public static final String GROUPING_KEY = "dh_key";

    public static final String PARQUET_FILE_EXTENSION = ".parquet";

    private static String minusParquetSuffix(final String s) {
        if (s.endsWith(PARQUET_FILE_EXTENSION)) {
            return s.substring(0, s.length() - PARQUET_FILE_EXTENSION.length());
        }
        return s;
    }

    public static Function<String, String> defaultGroupingFileName(final String path) {
        final String prefix = minusParquetSuffix(path);
        return columnName -> prefix + "_" + columnName + "_grouping.parquet";
    }

    /**
     * <p>Information about a writing destination (e.g. a particular output partition). Couples destination path,
     * input table data, and grouping information.
     */
    public static final class DestinationInfo {

        private final String outputPath;
        private final Table inputTable;
        private final Map<String, Map<?, long[]>> columnNameToGroupToRange;

        public DestinationInfo(@NotNull final String outputPath,
                               @NotNull final Table inputTable,
                               @NotNull final Map<String, Map<?, long[]>> columnNameToGroupToRange) {
            this.outputPath = outputPath;
            this.inputTable = inputTable;
            this.columnNameToGroupToRange = columnNameToGroupToRange;
        }

        /**
         * Get the output path name for this destination.
         *
         * @return The output path
         */
        public String getOutputPath() {
            return outputPath;
        }

        /**
         * Get the input table that should be read for this destination.
         *
         * @return The input table
         */
        public Table getInputTable() {
            return inputTable;
        }

        /**
         * Get a map from column name to the column's "group to range" map.
         *
         * @return Get this destination's grouping information
         */
        public Map<String, Map<?, long[]>> getColumnNameToGroupToRange() {
            return columnNameToGroupToRange;
        }
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t                   The table to write
     * @param path                The destination path
     * @param incomingMeta        A map of metadata values to be stores in the file footer
     * @param groupingPathFactory
     * @param groupingColumns     List of columns the tables are grouped by (the write operation will store the grouping info)
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to unsupported types)
     * @throws IOException            For file writing related errors
     */
    public static void write(
            Table t, String path, Map<String, String> incomingMeta, Function<String, String> groupingPathFactory, String... groupingColumns
    ) throws SchemaMappingException, IOException {
        write(t, t.getDefinition(), ParquetInstructions.EMPTY, path, incomingMeta, groupingPathFactory, groupingColumns);
    }

    public static void write(Table t, String path, Map<String, String> incomingMeta, String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, path, incomingMeta, defaultGroupingFileName(path), groupingColumns);
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t                    The table to write
     * @param definition           Table definition
     * @param writeInstructions    Write instructions for customizations while writing
     * @param destPathName                 The destination path
     * @param incomingMeta         A map of metadata values to be stores in the file footer
     * @param groupingPathFactory
     * @param groupingColumns     List of columns the tables are grouped by (the write operation will store the grouping info)
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to unsupported types)
     * @throws IOException            For file writing related errors
     */
    public static void write(
            final Table t,
            final TableDefinition definition,
            final ParquetInstructions writeInstructions,
            final String destPathName,
            final Map<String, String> incomingMeta,
            final Function<String, String> groupingPathFactory,
            final String... groupingColumns
    ) throws SchemaMappingException, IOException {
        final TableInfo.Builder tableInfoBuilder = TableInfo.builder();
        ArrayList<String> cleanupPaths = null;
        try {
            if (groupingColumns.length > 0) {
                cleanupPaths = new ArrayList<>(groupingColumns.length);
                final Table[] auxiliaryTables = Arrays.stream(groupingColumns).map(columnName -> groupingAsTable(t, columnName)).toArray(Table[]::new);
                final Path destDirPath = Paths.get(destPathName).getParent();
                for (int gci = 0; gci < auxiliaryTables.length; ++gci) {
                    final String groupingPath = groupingPathFactory.apply(groupingColumns[gci]);
                    cleanupPaths.add(groupingPath);
                    tableInfoBuilder.addGroupingColumns(GroupingColumnInfo.of(groupingColumns[gci], destDirPath.relativize(Paths.get(groupingPath)).toString()));
                    write(auxiliaryTables[gci], auxiliaryTables[gci].getDefinition(), writeInstructions, groupingPath, Collections.emptyMap());
                }
            }
            write(t, definition, writeInstructions, destPathName, incomingMeta, tableInfoBuilder);
        }
        catch (Exception e) {
            if (cleanupPaths != null) {
                for (String cleanupPath : cleanupPaths) {
                    try {
                        new File(cleanupPath).delete();
                    }
                    catch (Exception x) {
                        // ignore.
                    }
                }
            }
            throw e;
        }
    }

    public static void write(
            final Table t, final TableDefinition definition, final ParquetInstructions writeInstructions, final String path,
            final Map<String, String> incomingMeta, final String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, definition, writeInstructions, path, incomingMeta, defaultGroupingFileName(path), groupingColumns);
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param table              The table to write
     * @param definition         The table definition
     * @param writeInstructions  Write instructions for customizations while writing
     * @param path               The destination path
     * @param tableMeta          A map of metadata values to be stores in the file footer
     * @param tableInfoBuilder   A partially-constructed builder for the metadata object
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to unsupported types)
     * @throws IOException            For file writing related errors
     */
    public static void write(
            final Table table,
            final TableDefinition definition,
            final ParquetInstructions writeInstructions,
            final String path,
            final Map<String, String> tableMeta,
            final TableInfo.Builder tableInfoBuilder
    ) throws SchemaMappingException, IOException {

        final CompressionCodecName compressionCodecName = CompressionCodecName.valueOf(writeInstructions.getCompressionCodecName());
        ParquetFileWriter parquetFileWriter = getParquetFileWriter(definition, path, writeInstructions, tableMeta, tableInfoBuilder, compressionCodecName);

        final Table t = pretransformTable(table, definition);
        final long nrows = t.size();
        if (nrows > 0) {
            RowGroupWriter rowGroupWriter = parquetFileWriter.addRowGroup(nrows);
            // noinspection rawtypes
            for (Map.Entry<String, ? extends ColumnSource> nameToSource : t.getColumnSourceMap().entrySet()) {
                String name = nameToSource.getKey();
                ColumnSource<?> columnSource = nameToSource.getValue();
                try {
                    writeColumnSource(t.getIndex(), rowGroupWriter, name, columnSource, definition.getColumn(name), writeInstructions);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to write column " + name, e);
                }
            }
        }

        parquetFileWriter.close();
    }

    private static Table pretransformTable(final Table table, final TableDefinition definition) {
        List<SelectColumn> updateViewColumnsTransform = new ArrayList<>();
        List<SelectColumn> viewColumnsTransform = new ArrayList<>();
        Table t = table;
        for (ColumnDefinition<?> column : definition.getColumns()) {
            final String colName = column.getName();
            if (t.hasColumns(colName)) {
                if (StringSet.class.isAssignableFrom(column.getDataType())) {
                    updateViewColumnsTransform.add(FormulaColumn.createFormulaColumn(colName, colName + ".values()"));
                }
                viewColumnsTransform.add(new SourceColumn(colName));
            } else {
                //noinspection unchecked
                viewColumnsTransform.add(new NullSelectColumn(column.getDataType(), column.getComponentType(), colName));
            }
        }
        if (viewColumnsTransform.size() > 0) {
            t = t.view(viewColumnsTransform.toArray((SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY)));
        }
        if (updateViewColumnsTransform.size() > 0) {
            t = t.updateView(updateViewColumnsTransform.toArray(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY));
        }
        return t;
    }

    @NotNull
    private static ParquetFileWriter getParquetFileWriter(
            final TableDefinition definition,
            final String path,
            final ParquetInstructions writeInstructions,
            final Map<String, String> tableMeta,
            final TableInfo.Builder tableInfoBuilder,
            final CompressionCodecName codecName
    ) throws IOException {
        final MappedSchema mappedSchema = MappedSchema.create(definition, writeInstructions);
        final Map<String, String> extraMetaData = new HashMap<>(tableMeta);
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
            } else if (DbArrayBase.class.isAssignableFrom(column.getDataType())) {
                columnInfoBuilder.specialType(ColumnTypeInfo.SpecialType.Vector);
                usedColumnInfo = true;
            }
            if (usedColumnInfo) {
                tableInfoBuilder.addColumnTypes(columnInfoBuilder.build());
            }
        }
        extraMetaData.put(METADATA_KEY, tableInfoBuilder.build().serializeToJSON());
        return new ParquetFileWriter(path, TrackedSeekableChannelsProvider.getInstance(), PAGE_SIZE,
                new HeapByteBufferAllocator(), mappedSchema.getParquetSchema(), codecName, extraMetaData);
    }

    private static void writeColumnSource(
            final Index tableIndex,
            final RowGroupWriter rowGroupWriter,
            final String name,
            final ColumnSource columnSourceIn,
            final ColumnDefinition columnDefinition,
            final ParquetInstructions writeInstructions
    ) throws IllegalAccessException, IOException {
        Index index = tableIndex;
        ColumnSource columnSource = columnSourceIn;
        ColumnSource lengthSource = null;
        Index lengthIndex = null;
        int targetSize = getTargetSize(columnSource.getType());
        Supplier<Integer> rowStepGetter;
        Supplier<Integer> valuesStepGetter;
        int stepsCount;
        if (columnSource.getComponentType() != null
                && !CodecLookup.explicitCodecPresent(writeInstructions.getCodecName(columnDefinition.getName()))
                && !CodecLookup.codecRequired(columnDefinition)) {
            targetSize = getTargetSize(columnSource.getComponentType());
            HashMap<String, ColumnSource> columns = new HashMap<>();
            columns.put("array", columnSource);
            Table t = new QueryTable(index, columns);
            lengthSource = t.view("len= ((Object)array) == null?null:(int)array." + (DbArrayBase.class.isAssignableFrom(columnSource.getType()) ? "size()" : "length")).getColumnSource("len");
            lengthIndex = index;
            List<Integer> valueChunkSize = new ArrayList<>();
            List<Integer> originalChunkSize = new ArrayList<>();
            int runningSize = 0;
            int originalRowsCount = 0;
            try (final ChunkSource.GetContext context = lengthSource.makeGetContext(LOCAL_CHUNK_SIZE);
                 final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
                while (it.hasMore()) {
                    OrderedKeys ok = it.getNextOrderedKeysWithLength(LOCAL_CHUNK_SIZE);
                    IntChunk<Values> chunk = (IntChunk<Values>) lengthSource.getChunk(context, ok);
                    for (int i = 0; i < chunk.size(); i++) {
                        if (chunk.get(i) != Integer.MIN_VALUE) {
                            if (runningSize + chunk.get(i) > targetSize || originalRowsCount + 1 > targetSize) {
                                if (runningSize > targetSize) {
                                    targetSize = chunk.get(i);
                                }
                                valueChunkSize.add(runningSize);
                                originalChunkSize.add(originalRowsCount);
                                originalRowsCount = 0;
                                runningSize = 0;
                            }
                            runningSize += chunk.get(i);
                        }
                        originalRowsCount++;
                    }
                }
            }
            if (originalRowsCount > 0) {
                valueChunkSize.add(runningSize);
                originalChunkSize.add(originalRowsCount);
            }
            rowStepGetter = new Supplier<Integer>() {
                int step;

                @Override
                public Integer get() {
                    return originalChunkSize.get(step++);
                }
            };
            valuesStepGetter = new Supplier<Integer>() {
                int step;

                @Override
                public Integer get() {
                    return valueChunkSize.get(step++);
                }
            };
            stepsCount = valueChunkSize.size();
            Table array = t.ungroup("array");
            index = array.getIndex();
            columnSource = array.getColumnSource("array");
        } else {
            int finalTargetSize = targetSize;
            rowStepGetter = valuesStepGetter = () -> finalTargetSize;
            stepsCount = (int) (index.size()/finalTargetSize + ((index.size() % finalTargetSize) == 0?0:1));
        }
        Class columnType = columnSource.getType();
        if (columnType == DBDateTime.class) {
            columnSource = ReinterpretUtilities.dateTimeToLongSource(columnSource);
            columnType = columnSource.getType();
        }
        if (columnType == Boolean.class) {
            columnSource = ReinterpretUtilities.booleanToByteSource(columnSource);
        }
        ColumnWriter columnWriter = rowGroupWriter.addColumn(name);

        boolean usedDictionary = false;
        if (supportsDictionary(columnSource.getType())) {
            final boolean useDictionaryHint = writeInstructions.useDictionary(columnDefinition.getName());
            final int maxKeys = useDictionaryHint ? Integer.MAX_VALUE : writeInstructions.getMaximumDictionaryKeys();
            final class DictionarySizeExceededException extends RuntimeException {}
            try {
                final List<IntBuffer> buffersPerPage = new ArrayList<>();
                final Function<Integer, Object[]> keyArrayBuilder = getKeyArrayBuilder(columnSource.getType());
                final Function<Object, Object> toParquetPrimitive = getToParquetConversion(columnSource.getType());
                final MutableObject<Object[]> keys = new MutableObject<>(keyArrayBuilder.apply(Math.min(INITIAL_DICTIONARY_SIZE, maxKeys)));
                final Map<Object, Integer> keyToPos = new HashMap<>();
                final MutableInt keyCount = new MutableInt(0);
                final MutableBoolean hasNulls = new MutableBoolean(false);
                try (final ChunkSource.GetContext context = columnSource.makeGetContext(targetSize);
                     final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
                    for (int step = 0; step < stepsCount; step++) {
                        final OrderedKeys ok = it.getNextOrderedKeysWithLength(valuesStepGetter.get());
                        //noinspection unchecked
                        final ObjectChunk<?, Values> chunk = (ObjectChunk<?, Values>) columnSource.getChunk(context, ok);
                        final IntBuffer posInDictionary = IntBuffer.allocate((int) ok.size());
                        for (int vi = 0; vi < chunk.size(); ++vi) {
                            posInDictionary.put(keyToPos.computeIfAbsent(chunk.get(vi), o -> {
                                if (o == null) {
                                    hasNulls.setValue(true);
                                    return Integer.MIN_VALUE;
                                }
                                if (keyCount.intValue() == keys.getValue().length) {
                                    if (keyCount.intValue() == maxKeys) {
                                        throw new DictionarySizeExceededException();
                                    }
                                    keys.setValue(Arrays.copyOf(keys.getValue(), (int) Math.min(keyCount.intValue() * 2L, maxKeys)));
                                }
                                keys.getValue()[keyCount.intValue()] = toParquetPrimitive.apply(o);
                                Integer result = keyCount.getValue();
                                keyCount.increment();
                                return result;
                            }));

                        }
                        buffersPerPage.add(posInDictionary);
                    }
                }
                List<IntBuffer> repeatCount = null;
                if (lengthSource != null) {
                    repeatCount = new ArrayList<>();
                    try (final ChunkSource.GetContext context = lengthSource.makeGetContext(targetSize);
                         final OrderedKeys.Iterator it = lengthIndex.getOrderedKeysIterator()) {
                        while (it.hasMore()) {
                            final OrderedKeys ok = it.getNextOrderedKeysWithLength(rowStepGetter.get());
                            final IntChunk chunk = (IntChunk) lengthSource.getChunk(context, ok);
                            final IntBuffer newBuffer = IntBuffer.allocate(chunk.size());
                            chunk.copyToTypedBuffer(0, newBuffer, 0, chunk.size());
                            newBuffer.limit(chunk.size());
                            repeatCount.add(newBuffer);
                        }
                    }
                }
                columnWriter.addDictionaryPage(keys.getValue(), keyCount.intValue());
                final Iterator<IntBuffer> repeatCountIt = repeatCount == null ? null : repeatCount.iterator();
                for (final IntBuffer intBuffer : buffersPerPage) {
                    intBuffer.flip();
                    if (lengthSource != null) {
                        columnWriter.addVectorPage(intBuffer, repeatCountIt.next(), intBuffer.remaining(), Integer.MIN_VALUE);
                    } else if (hasNulls.getValue()) {
                        columnWriter.addPage(intBuffer, Integer.MIN_VALUE, intBuffer.remaining());
                    } else {
                        columnWriter.addPageNoNulls(intBuffer, intBuffer.remaining());
                    }
                }
                usedDictionary = true;
            } catch (DictionarySizeExceededException ignored) {
            }
        }
        if (!usedDictionary) {
            //noinspection unchecked
            try (final TransferObject<?> transferObject = getDestinationBuffer(columnSource, columnDefinition, targetSize, columnType, writeInstructions)) {
                final boolean supportNulls = supportNulls(columnType);
                final Object bufferToWrite = transferObject.getBuffer();
                final Object nullValue = getNullValue(columnType);
                try (final OrderedKeys.Iterator lengthIndexIt = lengthIndex != null ? lengthIndex.getOrderedKeysIterator() : null;
                     final ChunkSource.GetContext lengthSourceContext = lengthSource != null ? lengthSource.makeGetContext(targetSize) : null;
                     final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
                    final IntBuffer repeatCount = lengthSource != null ? IntBuffer.allocate(targetSize) : null;
                    for (int step = 0; step < stepsCount; ++step) {
                        final OrderedKeys ok = it.getNextOrderedKeysWithLength(valuesStepGetter.get());
                        transferObject.fetchData(ok);
                        transferObject.propagateChunkData();
                        if (lengthIndexIt != null) {
                            final IntChunk lenChunk = (IntChunk) lengthSource.getChunk(lengthSourceContext, lengthIndexIt.getNextOrderedKeysWithLength(rowStepGetter.get()));
                            lenChunk.copyToTypedBuffer(0, repeatCount, 0, lenChunk.size());
                            repeatCount.limit(lenChunk.size());
                            columnWriter.addVectorPage(bufferToWrite, repeatCount, transferObject.rowCount(), nullValue);
                            repeatCount.clear();
                        } else if (supportNulls) {
                            columnWriter.addPage(bufferToWrite, nullValue, transferObject.rowCount());
                        } else {
                            columnWriter.addPageNoNulls(bufferToWrite, transferObject.rowCount());
                        }
                    }
                }
            }
        }
        columnWriter.close();
    }

    private static Function<Object, Object> getToParquetConversion(Class type) {
        if (type == String.class) {
            //noinspection unchecked
            return (Function) (Function<String, Binary>) Binary::fromString;
        }
        throw new UnsupportedOperationException("Dictionary storage not supported for " + type);
    }

    private static Function<Integer, Object[]> getKeyArrayBuilder(Class type) {
        if (type == String.class) {
            return Binary[]::new;
        }
        throw new UnsupportedOperationException("Dictionary storage not supported for " + type);
    }

    private static boolean supportsDictionary(Class<?> dataType) {
        return dataType == String.class;
    }

    private static Object getNullValue(Class columnType) {
        if (columnType == Boolean.class) {
            return (byte) -1;
        } else if (columnType == char.class) {
            return (int)QueryConstants.NULL_CHAR;
        } else if (columnType == byte.class) {
            return QueryConstants.NULL_BYTE;
        } else if (columnType == short.class) {
            return QueryConstants.NULL_SHORT;
        } else if (columnType == int.class) {
            return QueryConstants.NULL_INT;
        } else if (columnType == long.class) {
            return QueryConstants.NULL_LONG;
        } else if (columnType == float.class) {
            return QueryConstants.NULL_FLOAT;
        } else if (columnType == double.class) {
            return QueryConstants.NULL_DOUBLE;
        }
        return null;
    }

    private static boolean supportNulls(Class columnType) {
        return !columnType.isPrimitive();
    }

    private static int getTargetSize(Class columnType) throws IllegalAccessException {
        if (columnType == Boolean.class) {
            return PAGE_SIZE * 8;
        }
        if (columnType == short.class || columnType == char.class || columnType == byte.class) {
            return PAGE_SIZE / Integer.BYTES;
        }
        if (columnType == String.class) {
            return PAGE_SIZE / Integer.BYTES;
        }
        Field bytesCountField = null;
        try {
            bytesCountField = TypeUtils.getBoxedType(columnType).getField("BYTES");
            return PAGE_SIZE / ((Integer) bytesCountField.get(null));
        } catch (NoSuchFieldException e) {
            return PAGE_SIZE / 8;//We assume the baseline and go from there
        }
    }


    private static <DATA_TYPE> TransferObject getDestinationBuffer(
            final ColumnSource<DATA_TYPE> columnSource,
            final ColumnDefinition<DATA_TYPE> columnDefinition,
            final int targetSize,
            final Class<DATA_TYPE> columnType,
            final ParquetInstructions instructions) {
        if (int.class.equals(columnType)) {
            int[] array = new int[targetSize];
            WritableIntChunk<Values> chunk = WritableIntChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, IntBuffer.wrap(array), targetSize);
        } else if (long.class.equals(columnType)) {
            long[] array = new long[targetSize];
            WritableLongChunk<Values> chunk = WritableLongChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, LongBuffer.wrap(array), targetSize);
        } else if (double.class.equals(columnType)) {
            double[] array = new double[targetSize];
            WritableDoubleChunk<Values> chunk = WritableDoubleChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, DoubleBuffer.wrap(array), targetSize);
        } else if (float.class.equals(columnType)) {
            float[] array = new float[targetSize];
            WritableFloatChunk<Values> chunk = WritableFloatChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, FloatBuffer.wrap(array), targetSize);
        } else if (Boolean.class.equals(columnType)) {
            byte[] array = new byte[targetSize];
            WritableByteChunk<Values> chunk = WritableByteChunk.writableChunkWrap(array);
            return new PrimitiveTransfer<>(columnSource, chunk, ByteBuffer.wrap(array), targetSize);
        } else if (short.class.equals(columnType)) {
            return new ShortTransfer(columnSource, targetSize);
        } else if (char.class.equals(columnType)) {
            return new CharTransfer(columnSource, targetSize);
        } else if (byte.class.equals(columnType)) {
            return new ByteTransfer(columnSource, targetSize);
        } else if (String.class.equals(columnType)) {
            return new StringTransfer(columnSource, targetSize);
        }
        final ObjectCodec<DATA_TYPE> codec = CodecLookup.lookup(columnDefinition, instructions);
        return new CodecTransfer(columnSource, codec, targetSize);
    }

    interface TransferObject<B> extends Context{

        void propagateChunkData();

        B getBuffer();

        int rowCount();

        void fetchData(OrderedKeys ok);
    }

    static class PrimitiveTransfer<C extends WritableChunk<Values>, B extends Buffer> implements TransferObject<B> {

        private final C chunk;
        private final B buffer;
        private final ColumnSource columnSource;
        private ChunkSource.FillContext context;

        PrimitiveTransfer(ColumnSource columnSource, C chunk, B buffer, int targetSize) {
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
        public void fetchData(OrderedKeys ok) {
            columnSource.fillChunk(context, chunk, ok);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class ShortTransfer implements TransferObject<IntBuffer> {

        private ShortChunk<Values> chunk;
        private final IntBuffer buffer;
        private final ColumnSource columnSource;
        private final ChunkSource.GetContext context;

        ShortTransfer(ColumnSource columnSource, int targetSize) {

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
        public void fetchData(OrderedKeys ok) {
            chunk = (ShortChunk<Values>) columnSource.getChunk(context, ok);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class CharTransfer implements TransferObject<IntBuffer> {

        private final ColumnSource columnSource;
        private final ChunkSource.GetContext context;
        private CharChunk<Values> chunk;
        private final IntBuffer buffer;

        CharTransfer(ColumnSource columnSource, int targetSize) {
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
        public void fetchData(OrderedKeys ok) {
            chunk = (CharChunk<Values>) columnSource.getChunk(context, ok);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class ByteTransfer implements TransferObject<IntBuffer> {

        private ByteChunk<Values> chunk;
        private final IntBuffer buffer;
        private final ColumnSource columnSource;
        private final ChunkSource.GetContext context;

        ByteTransfer(ColumnSource columnSource, int targetSize) {
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
        public void fetchData(OrderedKeys ok) {
            chunk = (ByteChunk<Values>) columnSource.getChunk(context, ok);
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
        private final ColumnSource columnSource;


        StringTransfer(ColumnSource columnSource, int targetSize) {
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
        public void fetchData(OrderedKeys ok) {
            chunk = (ObjectChunk<String, Values>) columnSource.getChunk(context, ok);
        }

        @Override
        public void close() {
            context.close();
        }
    }

    static class CodecTransfer implements TransferObject<Binary[]> {

        private final ChunkSource.GetContext context;
        private final ObjectCodec codec;
        private ObjectChunk<Object, Values> chunk;
        private final Binary[] buffer;
        private final ColumnSource columnSource;


        CodecTransfer(ColumnSource columnSource, ObjectCodec codec, int targetSize) {
            this.columnSource = columnSource;
            this.buffer = new Binary[targetSize];
            context = this.columnSource.makeGetContext(targetSize);
            this.codec = codec;
        }

        @Override
        public void propagateChunkData() {
            for (int i = 0; i < chunk.size(); i++) {
                Object value = chunk.get(i);
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
        public void fetchData(OrderedKeys ok) {
            chunk = (ObjectChunk<Object, Values>) columnSource.getChunk(context, ok);
        }

        @Override
        public void close() {
            context.close();
        }
    }


    private static boolean isRange(Index index) {
        return index.size() == (index.lastKey() - index.firstKey() + 1);
    }


    public static class RangeCollector {
        private boolean firstStep = true;
        private Object lastValue;
        TLongArrayList beginPos = new TLongArrayList();
        TLongArrayList endPos = new TLongArrayList();
        private long pos = 0;

        public boolean next(Object current) {
            boolean result = firstStep | !Objects.equals(lastValue, current);
            lastValue = current;
            if (result) {
                if (!firstStep) {
                    endPos.add(pos);
                } else {
                    firstStep = false;
                }
                beginPos.add(pos);
            }
            pos++;
            return result;
        }

        public void close() {
            if (pos > 0) {
                endPos.add(pos);
            }
        }

        public long[] beginPos() {
            return beginPos.toArray();
        }

        public long[] endPos() {
            return endPos.toArray();
        }
    }


    private static Table groupingAsTable(Table tableToSave, String columnName) {
        Map<?, Index> grouping = tableToSave.getIndex().getGrouping(tableToSave.getColumnSource(columnName));
        RangeCollector collector;
        QueryScope.getScope().putParam("__range_collector_" + columnName + "__", collector = new RangeCollector());
        Table firstOfTheKey = tableToSave.view(columnName).where("__range_collector_" + columnName + "__.next(" + columnName + ")");

        Table contiguousOccurrences = firstOfTheKey.countBy("c", columnName).where("c != 1");
        if (contiguousOccurrences.size() != 0) {
            throw new RuntimeException("Disk grouping is not possible for column because some indices are not contiguous");
        }
        Object columnValues = firstOfTheKey.getColumn(columnName).getDirect();
        collector.close();
        return new InMemoryTable(new String[]{GROUPING_KEY, BEGIN_POS, END_POS}, new Object[]{columnValues, collector.beginPos(), collector.endPos()});
    }

    public static class SomeSillyTest implements Serializable {
        private static final long serialVersionUID = 6668727512367188538L;
        final int value;

        public SomeSillyTest(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "SomeSillyTest{" +
                    "value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SomeSillyTest)) {
                return false;
            }
            return value == ((SomeSillyTest) obj).value;
        }
    }

    private static Table getTableFlat() {
        QueryLibrary.importClass(SomeSillyTest.class);
        return TableTools.emptyTable(10).select(
                "someStringColumn = i % 10 == 0?null:(`` + (i % 101))",
                "nonNullString = `` + (i % 60)",
                "someIntColumn = i",
                "someNullableInts = i%5 != 0?i:null",
                "someLongColumn = ii",
                "someDoubleColumn = i*1.1",
                "someFloatColumn = (float)(i*1.1)",
                "someBoolColum = i % 3 == 0?true:i%3 == 1?false:null",
                "someShortColumn = (short)i",
                "someByteColumn = (byte)i",
                "someCharColumn = (char)i",
                "someTime = DBDateTime.now() + i",
                "someKey = `` + (int)(i /100)",
                "nullKey = i < -1?`123`:null",
                "someSerializable = new SomeSillyTest(i)"
        );
    }

    private static Table getGroupedTable() {
        Table t = getTableFlat();
        QueryLibrary.importClass(StringSetArrayWrapper.class);
        Table result = t.by("groupKey = i % 100 + (int)(i/10)").update("someStringSet = new StringSetArrayWrapper(nonNullString)");
        TableTools.show(result);
        return result;
    }
}

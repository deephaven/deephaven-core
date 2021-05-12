package io.deephaven.db.v2.parquet;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.StringSetArrayWrapper;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ExternalizableCodec;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.codec.SerializableCodec;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.parquet.ColumnWriter;
import io.deephaven.parquet.ParquetFileWriter;
import io.deephaven.parquet.RowGroupWriter;
import io.deephaven.parquet.utils.LocalFSChannelProvider;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * API for writing DH tables in parquet format
 */
public class ParquetTableWriter {

    private static final int PAGE_SIZE = 1 << 20;
    private static final int INITIAL_DICTIONARY_SIZE = 1 << 8;
    public static final String GROUPING = "grouping";
    public static final String _CODEC_NAME_PREFIX_ = "__codec_name__";
    public static final String _CODEC_ARGS_PREFIX_ = "__codec_args__";
    public static final String SPECIAL_TYPE_NAME_PREFIX_ = "__special_type__";
    public static final String STRING_SET_SPECIAL_TYPE = "StringSet";
    private static final int LOCAL_CHUNK_SIZE = 1024;
    public static final IntBuffer EMPTY_INT_BUFFER = IntBuffer.allocate(0);
    public static final String BEGIN_POS = "__begin_pos__";
    public static final String END_POS = "__end_pos__";
    public static final String GROUPING_KEY = "__key__";
    public static Function<String, String> defaultGroupingFileName = columnName -> columnName + "_grouping.parquet";


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
     * @param destinationInfos Destination information coupling input data, output path, and grouping metadata for each desired result table
     * @param codecName        Compression codec to use
     * @param definition       The definition to use for the output tables
     * @param parallelColumns  The maximum number of columns that should be written in parallel
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to unsupported types)
     * @throws IOException            For file writing related errors
     */
    public static void write(@NotNull final DestinationInfo[] destinationInfos, @NotNull final CompressionCodecName codecName, @NotNull final TableDefinition definition, final int parallelColumns) throws SchemaMappingException, IOException {
        ParquetFileWriter[] writers = new ParquetFileWriter[destinationInfos.length];
        RowGroupWriter[] rowGroupWriters = new RowGroupWriter[destinationInfos.length];
        Table tablesToWrite[] = Arrays.stream(destinationInfos).map(d -> d.inputTable).toArray(Table[]::new);
        for (int i = 0; i < destinationInfos.length; i++) {
            DestinationInfo destinationInfo = destinationInfos[i];

            Map<String, String> tableMeta = new HashMap<>();
            tableMeta.put(GROUPING, String.join(",", definition.getGroupingColumnNamesArray()));
            writers[i] = getParquetFileWriter(tablesToWrite[i], definition, destinationInfo.getOutputPath() + "/table.parquet", tableMeta, codecName);
            rowGroupWriters[i] = writers[i].addRowGroup(tablesToWrite[i].size());
            tablesToWrite[i] = pretransformTable(tablesToWrite[i], definition);
            for (Map.Entry<String, Map<?, long[]>> columnGrouping : destinationInfo.columnNameToGroupToRange.entrySet()) {
                String columnName = columnGrouping.getKey();
                Class keyType = definition.getColumn(columnName).getDataType();
                Map<String, ColumnSource> sourceMap = new HashMap<>();
                Map<?, long[]> keyRangeMap = columnGrouping.getValue();
                sourceMap.put(GROUPING_KEY, ArrayBackedColumnSource.getMemoryColumnSource(keyRangeMap.keySet().toArray((Object[]) Array.newInstance(TypeUtils.getBoxedType(keyType), 0))));
                sourceMap.put(BEGIN_POS, ArrayBackedColumnSource.getMemoryColumnSource(keyRangeMap.values().stream().mapToLong(range -> range[0]).toArray()));
                sourceMap.put(END_POS, ArrayBackedColumnSource.getMemoryColumnSource(keyRangeMap.values().stream().mapToLong(range -> range[1]).toArray()));
                QueryTable groupingTable = new QueryTable(Index.FACTORY.getIndexByRange(0, keyRangeMap.size() - 1), sourceMap);
                write(groupingTable, destinationInfo.getOutputPath() + "/" + defaultGroupingFileName.apply(columnName), Collections.emptyMap(), codecName, groupingTable.getDefinition());
            }
        }
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(destinationInfos.length);
        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(16);
        for (String columnName : definition.getColumnNames()) {
            Future tasks[] = new Future[destinationInfos.length];
            for (int i = 0; i < destinationInfos.length; i++) {
                RowGroupWriter rowGroupWriter = rowGroupWriters[i];
                Table table = tablesToWrite[i];
                tasks[i] = threadPoolExecutor.submit(() -> {
                    try {
                        writeColumnSource(table.getIndex(), rowGroupWriter, columnName, table.getColumnSource(columnName), definition.getColumn(columnName));
                    } catch (IllegalAccessException  | IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            for (Future task : tasks) {
                try {
                    task.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        for (ParquetFileWriter writer : writers) {
            writer.close();
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
    public static void write(Table t, String path, Map<String, String> incomingMeta, Function<String, String> groupingPathFactory, String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, path, incomingMeta, CompressionCodecName.UNCOMPRESSED, t.getDefinition(), groupingPathFactory, groupingColumns);
    }

    public static void write(Table t, String path, Map<String, String> incomingMeta, String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, path, incomingMeta, defaultGroupingFileName, groupingColumns);
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t                   The table to write
     * @param path                The destination path
     * @param incomingMeta        A map of metadata values to be stores in the file footer
     * @param codecName           Compression codec to use
     * @param definition
     * @param groupingPathFactory
     * @param groupingColumns     List of columns the tables are grouped by (the write operation will store the grouping info)
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to unsupported types)
     * @throws IOException            For file writing related errors
     */
    public static void write(Table t, String path, Map<String, String> incomingMeta, CompressionCodecName codecName, TableDefinition definition, Function<String, String> groupingPathFactory, String... groupingColumns) throws SchemaMappingException, IOException {
        Map<String, String> tableMeta = new HashMap<>(incomingMeta);
        Table[] auxiliaryTables = Arrays.stream(groupingColumns).map(columnName -> groupingAsTable(t, columnName)).toArray(Table[]::new);
        tableMeta.put(GROUPING, String.join(",", groupingColumns));
        for (int i = 0; i < auxiliaryTables.length; i++) {
            write(auxiliaryTables[i], auxiliaryTables[i].getDefinition(), groupingPathFactory.apply(groupingColumns[i]), Collections.emptyMap(), codecName);
        }
        write(t, definition, path, tableMeta, codecName);
    }

    public static void write(Table t, String path, Map<String, String> incomingMeta, CompressionCodecName codecName, TableDefinition definition, String... groupingColumns) throws SchemaMappingException, IOException {
        write(t, path, incomingMeta, codecName, definition, defaultGroupingFileName, groupingColumns);
    }

    /**
     * Writes a table in parquet format under a given path
     *
     * @param t          The table to write
     * @param definition
     * @param path       The destination path
     * @param tableMeta  A map of metadata values to be stores in the file footer
     * @param codecName  Name oif the codec for compression
     * @throws SchemaMappingException Error creating a parquet table schema for the given table (likely due to unsupported types)
     * @throws IOException            For file writing related errors
     */
    public static void write(Table t, TableDefinition definition, String path, Map<String, String> tableMeta, CompressionCodecName codecName) throws SchemaMappingException, IOException {

        ParquetFileWriter parquetFileWriter = getParquetFileWriter(t, definition, path, tableMeta, codecName);

        t = t.view(definition.getColumnNamesArray());
        t = pretransformTable(t, definition);

        RowGroupWriter rowGroupWriter = parquetFileWriter.addRowGroup(t.size());
        for (Map.Entry<String, ? extends ColumnSource> nameToSource : t.getColumnSourceMap().entrySet()) {
            String name = nameToSource.getKey();
            ColumnSource columnSource = nameToSource.getValue();
            try {
                writeColumnSource(t.getIndex(), rowGroupWriter, name, columnSource, definition.getColumn(name));
            } catch (IllegalAccessException  e) {
                throw new RuntimeException("Failed to write column " + name, e);
            }
        }

        parquetFileWriter.close();
    }

    private static Table pretransformTable(Table t, TableDefinition definition) {
        List<String> columnsTransform = new ArrayList<>();
        for (ColumnDefinition column : definition.getColumns()) {
            if (StringSet.class.isAssignableFrom(column.getDataType())) {
                columnsTransform.add(column.getName() + " = " + column.getName() + ".values()");
            }
        }
        if (columnsTransform.size() > 0) {
            t = t.updateView(columnsTransform.toArray(new String[0]));
        }
        return t;
    }

    @NotNull
    private static ParquetFileWriter getParquetFileWriter(Table t, TableDefinition definition, String path, Map<String, String> tableMeta, CompressionCodecName codecName) throws SchemaMappingException, IOException {
        MappedSchema mappedSchema = MappedSchema.create(definition);
        Map<String, String> extraMetaData = new HashMap<>(tableMeta);
        for (ColumnDefinition column : definition.getColumns()) {
            Pair<String, String> codecData = TypeInfos.getCodecAndArgs(column);
            if (codecData != null) {
                extraMetaData.put(_CODEC_NAME_PREFIX_ + column.getName(), codecData.getLeft());
                extraMetaData.put(_CODEC_ARGS_PREFIX_ + column.getName(), codecData.getRight());
            }
            if (StringSet.class.isAssignableFrom(column.getDataType())) {
                extraMetaData.put(SPECIAL_TYPE_NAME_PREFIX_ + column.getName(), STRING_SET_SPECIAL_TYPE);
            }
        }
        return new ParquetFileWriter(path, new LocalFSChannelProvider(), PAGE_SIZE,
                new HeapByteBufferAllocator(), mappedSchema.getParquetSchema(), codecName, extraMetaData);
    }

    private static void writeColumnSource(Index index, RowGroupWriter rowGroupWriter, String name, ColumnSource columnSource, ColumnDefinition columnDefinition) throws IllegalAccessException, IOException {

        ColumnSource lengthSource = null;
        Index lengthIndex = null;
        int targetSize = getTargetSize(columnSource.getType());
        Supplier<Integer> rowStepGetter;
        Supplier<Integer> valuesStepGetter = null;
        int stepsCount;
        if (columnSource.getComponentType() != null && columnDefinition.getObjectCodecType() == ColumnDefinition.ObjectCodecType.DEFAULT) {
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

        if (supportsDictionary(columnSource)) {
            List<IntBuffer> buffersPerPage = new ArrayList<>();
            Function<Integer, Object[]> keyArrayBuilder = getKeyArrayBuilder(columnSource.getType());
            Function<Object, Object> toParquetPrimitive = getToParquetConversion(columnSource.getType());
            final Object[][] keys = {keyArrayBuilder.apply(INITIAL_DICTIONARY_SIZE)};
            Map<Object, Integer> keyToPos = new HashMap<>();
            final MutableInt keyCount = new MutableInt(0);
            final MutableBoolean hasNulls = new MutableBoolean(false);
            try (final ChunkSource.GetContext context = columnSource.makeGetContext(targetSize);
                 final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
                for (int step = 0; step < stepsCount; step++) {
                    OrderedKeys ok = it.getNextOrderedKeysWithLength(valuesStepGetter.get());
                    ObjectChunk<?, Values> chunk = (ObjectChunk<?, Values>) columnSource.getChunk(context, ok);
                    IntBuffer posInDictionary = IntBuffer.allocate((int) ok.size());
                    for (int i = 0; i < chunk.size(); i++) {
                        posInDictionary.put(keyToPos.computeIfAbsent(chunk.get(i), o -> {
                            if (o == null) {
                                hasNulls.setValue(true);
                                return Integer.MIN_VALUE;
                            }
                            if (keyCount.intValue() == keys[0].length) {
                                keys[0] = Arrays.copyOf(keys[0], keys[0].length * 2);
                            }
                            keys[0][keyCount.intValue()] = toParquetPrimitive.apply(o);
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
                        OrderedKeys ok = it.getNextOrderedKeysWithLength(rowStepGetter.get());
                        IntChunk chunk = (IntChunk) lengthSource.getChunk(context, ok);
                        IntBuffer newBuffer = IntBuffer.allocate(chunk.size());
                        chunk.copyToTypedBuffer(0, newBuffer, 0, chunk.size());
                        newBuffer.limit(chunk.size());
                        repeatCount.add(newBuffer);
                    }
                }
            }
            if (keyCount.intValue() == 0) {
                keys[0][0] = toParquetPrimitive.apply("");
                keyCount.increment();
            }
            columnWriter.addDictionaryPage(keys[0], keyCount.intValue());
            Iterator<IntBuffer> repeatCountIt = repeatCount == null ? null : repeatCount.iterator();
            for (IntBuffer intBuffer : buffersPerPage) {
                intBuffer.flip();
                if (lengthSource != null) {
                    columnWriter.addVectorPage(intBuffer, repeatCountIt.next(), intBuffer.remaining(), Integer.MIN_VALUE);
                } else if (hasNulls.getValue()) {
                    columnWriter.addPage(intBuffer, Integer.MIN_VALUE, intBuffer.remaining());
                } else {
                    columnWriter.addPageNoNulls(intBuffer, intBuffer.remaining());
                }
            }
        } else {
            try (final TransferObject transferObject = getDestinationBuffer(columnSource, columnDefinition, targetSize, columnType)) {
                boolean supportNulls = supportNulls(columnType);
                Object bufferToWrite = transferObject.getBuffer();
                Object nullValue = getNullValue(columnType);
                try (final OrderedKeys.Iterator lengthIndexIt = lengthIndex != null ? lengthIndex.getOrderedKeysIterator() : null;
                     final ChunkSource.GetContext lengthSourceContext = lengthSource != null ? lengthSource.makeGetContext(targetSize) : null;
                     final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
                    IntBuffer repeatCount = lengthSource != null ? IntBuffer.allocate(targetSize) : null;
                    for (int step = 0; step < stepsCount; step++) {
                        OrderedKeys ok = it.getNextOrderedKeysWithLength(valuesStepGetter.get());
                        transferObject.fetchData(ok);
                        transferObject.propagateChunkData();
                        if (lengthIndexIt != null) {
                            IntChunk lenChunk = (IntChunk) lengthSource.getChunk(lengthSourceContext, lengthIndexIt.getNextOrderedKeysWithLength(rowStepGetter.get()));
                            lenChunk.copyToTypedBuffer(0, repeatCount, 0, lenChunk.size());
                            repeatCount.limit(lenChunk.size());
                            columnWriter.addVectorPage(bufferToWrite, repeatCount, transferObject.rowCount(), nullValue);
                            repeatCount.clear();
                        } else if (supportNulls) {
                            try {
                                columnWriter.addPage(bufferToWrite, nullValue, transferObject.rowCount());
                            } catch (Exception e) {
                                throw e;
                            }
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

    private static boolean supportsDictionary(ColumnSource columnSource) {
        return columnSource.getType() == String.class;
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


    private static TransferObject getDestinationBuffer(ColumnSource columnSource, ColumnDefinition columnDefinition, int targetSize, Class columnType) {
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

        String objectCodecClass = columnDefinition.getObjectCodecClass();
        final ObjectCodec<Object> codec;
        if (objectCodecClass != null && !objectCodecClass.equals(ColumnDefinition.ObjectCodecType.DEFAULT.name())) {
            codec = CodecCache.DEFAULT.getCodec(objectCodecClass, columnDefinition.getObjectCodecArguments());
        } else if (Externalizable.class.isAssignableFrom(columnDefinition.getDataType())) {
            codec = CodecCache.DEFAULT.getCodec(ExternalizableCodec.class.getName(), columnDefinition.getDataType().getName());
        } else {
            codec = CodecCache.DEFAULT.getCodec(SerializableCodec.class.getName(), "");
        }
        return new CodecTransfer(columnSource, codec, targetSize);


        // throw new UnsupportedOperationException("Unsupported primitive type " + columnType.getName());
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

    public static void main(String[] args) throws IOException, SchemaMappingException {
        long start = System.nanoTime();
        Table tableToSave = getTableFlat();
        TableTools.show(tableToSave);
        System.out.println((System.nanoTime() - start) * 1.0 / 1000000000);
        System.out.println("Writing data");
        start = System.nanoTime();
        write(tableToSave, "table0.parquet", new HashMap<>(), CompressionCodecName.SNAPPY, tableToSave.getDefinition(), defaultGroupingFileName, new String[]{/*"someKey"*/});
        System.out.println((System.nanoTime() - start) * 1.0 / 1000000000);
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

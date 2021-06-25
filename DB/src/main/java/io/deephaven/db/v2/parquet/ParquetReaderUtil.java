package io.deephaven.db.v2.parquet;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.parquet.*;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.utils.CachedChannelProvider;
import io.deephaven.parquet.utils.LocalFSChannelProvider;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.IntBuffer;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ParquetReaderUtil {

    public static void main(String[] args) throws IOException {
        if (args[0].equals("-random")) {
            testRandom(args[1]);
        } else {
            readParquetFile(args[0]);
        }
    }

    @SuppressWarnings("WeakerAccess")
    @VisibleForTesting
    public static void readParquetFile(String filePath) throws IOException {
        ParquetFileReader pf = new ParquetFileReader(
                filePath, getChannelsProvider(), 0);
        System.out.println(pf.fileMetaData.getKey_value_metadata());
        ParquetMetadata pm = new ParquetMetadataConverter().fromParquetMetadata(pf.fileMetaData);

        MessageType schema = pf.getSchema();

        for (int i = 0; i < pf.rowGroupCount(); i++) {
            RowGroupReader rg = pf.getRowGroup(i);

            for (ColumnDescriptor column : schema.getColumns()) {
                long start = System.nanoTime();
                System.out.println(column + ":");
                LogicalTypeAnnotation logicalTypeAnnotation = column.getPrimitiveType().getLogicalTypeAnnotation();
                Function<Object, String> toString = null;
                Object nullValue = null;

                if (logicalTypeAnnotation == null) {
                    switch (column.getPrimitiveType().getPrimitiveTypeName()) {
                        case BOOLEAN:
                            toString = (data) -> toBooleanString((byte[]) data);
                            nullValue = (byte) -1;
                            break;
                        case INT32:
                            toString = (data) -> Arrays.toString((int[]) data);
                            nullValue = Integer.MIN_VALUE + 1;
                            break;
                        case INT64:
                            toString = (data) -> Arrays.toString((long[]) data);
                            nullValue = Long.MIN_VALUE + 1;
                            break;
                        case DOUBLE:
                            toString = (data) -> Arrays.toString((double[]) data);
                            nullValue = Double.MIN_VALUE + 1;
                            break;
                        case FLOAT:
                            toString = (data) -> Arrays.toString((float[]) data);
                            nullValue = Float.MIN_VALUE + 1;
                            break;
                        case BINARY:
                        case FIXED_LEN_BYTE_ARRAY:
                            nullValue = null;
                            Map<String, String> keyValueMetaData = pm.getFileMetaData().getKeyValueMetaData();
                            String codecName = keyValueMetaData.get(ParquetTableWriter.CODEC_NAME_PREFIX + column.getPath()[0]);
                            String codecParams = keyValueMetaData.get(ParquetTableWriter.CODEC_ARGS_PREFIX + column.getPath()[0]);
                            ObjectCodec<Object> codec = CodecCache.DEFAULT.getCodec(codecName, codecParams);
                            toString = (data) -> Arrays.toString((Object[]) convertArray((Binary[]) data, codec));
                            break;
                        case INT96:
                            throw new UncheckedDeephavenException("Unsupported type " + column.getPrimitiveType() + " for column " + Arrays.toString(column.getPath()));
                    }
                    toString = applyRepetitionLevel(rg, column, toString);
                    Function<Object, String> finalToString = toString;
                    readRowGroup(rg, nullValue, (data) -> System.out.println(cap(finalToString.apply(data))), Arrays.asList(column.getPath()));
                } else {
                    Optional<Object> result = logicalTypeAnnotation.accept(new ReadRowGroupVisitor(rg, column));
                    if (result.isPresent() && result.get() instanceof Exception) {
                        throw new UncheckedDeephavenException("Unable to read column " + Arrays.toString(column.getPath()), (Throwable) result.get());
                    }
                }
                System.out.println((System.nanoTime() - start) * 1.0 / 1000000000);
            }
        }
    }

    @FunctionalInterface
    public interface ColumnDefinitionConsumer {
        // objectWidth == -1 means not present.
        void accept(String name, Class<?> dataType, Class<?> componentType, boolean isGroupingColumn, String codecName, String codecArgs);
    }

    private static Class<?> loadClass(final String colName, final String desc, final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new UncheckedDeephavenException(
                    "Column " + colName + " with " + desc + "=" + className + " that can't be found in classloader.");
        }
    }

    /**
     * Obtain schema information from a parquet file
     *
     * @param filePath  Location for input parquet file
     * @param readInstructions  Parquet read instructions specifying transformations like column mappings and codecs.
     *                          Note a new read instructions based on this one may be returned by this method to provide necessary
     *                          transformations, eg, replacing unsupported characters like ' ' (space) in column names.
     * @param colDefConsumer  A ColumnDefinitionConsumer whose accept method would be called for each column in the file
     * @return Parquet read instructions, either the ones supplied or a new object based on the supplied with necessary
     *         transformations added.
     * @throws IOException if the specified file cannot be read
     */
    public static ParquetInstructions readParquetSchema(
            final String filePath,
            final ParquetInstructions readInstructions,
            final ColumnDefinitionConsumer colDefConsumer,
            final BiFunction<String, Set<String>, String> legalizeColumnNameFunc
    ) throws IOException {
        final ParquetFileReader pf = new ParquetFileReader(
                filePath, getChannelsProvider(), 0);
        final MessageType schema = pf.getSchema();
        final ParquetMetadata pm = new ParquetMetadataConverter().fromParquetMetadata(pf.fileMetaData);
        final Map<String, String> keyValueMetaData = pm.getFileMetaData().getKeyValueMetaData();
        final MutableObject<String> errorString = new MutableObject<>();
        final MutableObject<ColumnDescriptor> currentColumn = new MutableObject<>();
        final LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>> visitor = new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>>() {
            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                final ColumnDescriptor column = currentColumn.getValue();
                final String specialType = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + column.getPath()[0]);
                if (specialType != null) {
                    if (specialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                        return Optional.of(StringSet.class);
                    }
                    throw new UncheckedDeephavenException("Type " + column.getPrimitiveType()
                            + " for column " + Arrays.toString(column.getPath())
                            + " with unknown or incompatible special type " + specialType);
                } else {
                    return Optional.of(String.class);
                }
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
                errorString.setValue("MapLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
                errorString.setValue("ListLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
                errorString.setValue("EnumLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                errorString.setValue("DecimalLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
                errorString.setValue("DateLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
                errorString.setValue("TimeLogicalType,isAdjustedToUTC=" + timeLogicalType.isAdjustedToUTC());
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
                if (timestampLogicalType.isAdjustedToUTC()) {
                    switch(timestampLogicalType.getUnit()) {
                        case MILLIS:
                        case MICROS:
                        case NANOS:
                            return Optional.of(io.deephaven.db.tables.utils.DBDateTime.class);
                    }
                }
                errorString.setValue("TimestampLogicalType,isAdjustedToUTC=" + timestampLogicalType.isAdjustedToUTC() + ",unit=" + timestampLogicalType.getUnit());
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
                // Ensure this stays in sync with ReadOnlyParquetTableLocation.LogicalTypeVisitor.
                if (intLogicalType.isSigned()) {
                    switch (intLogicalType.getBitWidth()) {
                        case 64:
                            return Optional.of(long.class);
                        case 32:
                            return Optional.of(int.class);
                        case 16:
                            return Optional.of(short.class);
                        case 8:
                            return Optional.of(byte.class);
                        default:
                            // fallthrough.
                    }
                } else {
                    switch (intLogicalType.getBitWidth()) {
                        // uint16 maps to java's char;
                        // other unsigned types are promoted.
                        case 8:
                        case 16:
                            return Optional.of(char.class);
                        case 32:
                            return Optional.of(long.class);
                        default:
                            // fallthrough.
                    }
                }
                errorString.setValue("IntLogicalType,isSigned=" + intLogicalType.isSigned() + ",bitWidth=" + intLogicalType.getBitWidth());
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
                errorString.setValue("JsonLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
                errorString.setValue("BsonLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
                errorString.setValue("UUIDLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
                errorString.setValue("IntervalLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
                errorString.setValue("MapKeyValueType");
                return Optional.empty();
            }
        };

        final String csvGroupingCols = keyValueMetaData.get(ParquetTableWriter.GROUPING);
        Set<String> groupingCols = Collections.emptySet();
        if (csvGroupingCols != null && !csvGroupingCols.isEmpty()) {
            groupingCols = new HashSet<>(Arrays.asList(csvGroupingCols.split(",")));
        }

        ParquetInstructions.Builder instructionsBuilder = null;
        for (ColumnDescriptor column : schema.getColumns()) {
            currentColumn.setValue(column);
            final PrimitiveType primitiveType = column.getPrimitiveType();
            final LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
            final String parquetColumnName = column.getPath()[0];
            final String colName;
            final String mappedName = readInstructions.getColumnNameFromParquetColumnName(parquetColumnName);
            if (mappedName != null) {
                colName = mappedName;
            } else {
                final String legalized = legalizeColumnNameFunc.apply(
                        parquetColumnName,
                        (instructionsBuilder == null) ? Collections.emptySet() : instructionsBuilder.getTakenNames());
                if (!legalized.equals(parquetColumnName)) {
                    colName = legalized;
                    if (instructionsBuilder == null) {
                        instructionsBuilder = new ParquetInstructions.Builder(readInstructions);
                    }
                    instructionsBuilder.addColumnNameMapping(parquetColumnName, colName);
                } else {
                    colName = parquetColumnName;
                }
            }
            final boolean isGrouping = groupingCols.contains(colName);
            String codecName = keyValueMetaData.get(ParquetTableWriter.CODEC_NAME_PREFIX + colName);
            String codecArgs = keyValueMetaData.get(ParquetTableWriter.CODEC_ARGS_PREFIX + colName);
            final String codecType = keyValueMetaData.get(ParquetTableWriter.CODEC_DATA_TYPE_PREFIX + colName);
            if (codecType != null && !codecType.isEmpty()) {
                final Class<?> dataType = loadClass(colName, "codec type", codecType);
                final String codecComponentType = keyValueMetaData.get(ParquetTableWriter.CODEC_COMPONENT_TYPE_PREFIX + colName);
                final Class<?> componentType;
                if (codecComponentType == null || codecComponentType.isEmpty()) {
                    componentType = null;
                } else {
                    try {
                        componentType = ClassUtil.lookupClass(codecComponentType);
                    } catch (ClassNotFoundException e) {
                        throw new UncheckedDeephavenException(
                                "Column " + colName + " with codec component type " + codecComponentType +
                                        " that can't be found in classloader:" + e, e);
                    }
                }
                colDefConsumer.accept(colName, dataType, componentType, isGrouping, codecName, codecArgs);
                continue;
            }
            final boolean isArray = column.getMaxRepetitionLevel() > 0;
            if (logicalTypeAnnotation == null) {
                final PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
                switch (typeName) {
                    case BOOLEAN:
                        if (isArray) {
                            colDefConsumer.accept(colName, Boolean[].class, Boolean.class, isGrouping, codecName, codecArgs);
                        } else {
                            colDefConsumer.accept(colName, Boolean.class, null, isGrouping, codecName, codecArgs);
                        }
                        break;
                    case INT32:
                        if (isArray) {
                            colDefConsumer.accept(colName, int[].class, int.class, isGrouping, codecName, codecArgs);
                        } else {
                            colDefConsumer.accept(colName, int.class, null, isGrouping, codecName, codecArgs);
                        }
                        break;
                    case INT64:
                        if (isArray) {
                            colDefConsumer.accept(colName, long[].class, long.class, isGrouping, codecName, codecArgs);
                        } else {
                            colDefConsumer.accept(colName, long.class, null, isGrouping, codecName, codecArgs);
                        }
                        break;
                    case INT96:
                        colDefConsumer.accept(colName, DBDateTime.class, null, isGrouping, codecName, codecArgs);
                        break;
                    case DOUBLE:
                        if (isArray) {
                            colDefConsumer.accept(colName, double[].class, double.class, isGrouping, codecName, codecArgs);
                        } else {
                            colDefConsumer.accept(colName, double.class, null, isGrouping, codecName, codecArgs);
                        }
                        break;
                    case FLOAT:
                        if (isArray) {
                            colDefConsumer.accept(colName, float[].class, float.class, isGrouping, codecName, codecArgs);
                        } else {
                            colDefConsumer.accept(colName, float.class, null, isGrouping, codecName, codecArgs);
                        }
                        break;
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        final String specialType = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + colName);
                        final Supplier<String> exceptionTextSupplier = ()
                                -> "BINARY or FIXED_LEN_BYTE_ARRAY type " + column.getPrimitiveType()
                                    + " for column " + Arrays.toString(column.getPath());
                        if (specialType != null) {
                            if (specialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                                colDefConsumer.accept(colName, StringSet.class, null, isGrouping, codecName, codecArgs);
                            } else {
                                throw new UncheckedDeephavenException(exceptionTextSupplier.get()
                                        + " with unknown special type " + specialType);
                            }
                        }
                        if (codecName == null || codecName.isEmpty()) {
                            codecName = SimpleByteArrayCodec.class.getName();
                            codecArgs =  (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                ? Integer.toString(primitiveType.getTypeLength())
                                : null;
                        }
                        colDefConsumer.accept(colName, byte[].class, byte.class, isGrouping, codecName, codecArgs);
                        break;
                    default:
                        colDefConsumer.accept(colName, byte[].class, byte.class, isGrouping, codecName, codecArgs);
                        break;
                }
            } else {
                final Class<?> typeFromVisitor = logicalTypeAnnotation.accept(visitor).orElseThrow(() -> {
                    final String logicalTypeString = errorString.getValue();
                    return new UncheckedDeephavenException("Unable to read column " + Arrays.toString(column.getPath())
                            + ((logicalTypeString != null)
                            ? (logicalTypeString + " not supported")
                            : "no mappable logical type annotation found."));});
                if (!StringSet.class.isAssignableFrom(typeFromVisitor) && isArray) {
                    final Class<?> componentType = typeFromVisitor;
                    // On Java 12, replace by:  dataType = componentType.arrayType();
                    final Class<?> dataType = java.lang.reflect.Array.newInstance(componentType, 0).getClass();
                    colDefConsumer.accept(colName, dataType, componentType, isGrouping, codecName, codecArgs);
                } else {
                    colDefConsumer.accept(colName, typeFromVisitor, null, isGrouping, codecName, codecArgs);
                }
            }
        }
        if (instructionsBuilder == null) {
            return readInstructions;
        }
        return instructionsBuilder.build();
    }

    private static Object[] convertArray(Binary[] rawData, ObjectCodec<?> codec) {
        Object[] result = new Object[rawData.length];
        for (int i = 0; i < rawData.length; i++) {
            if (rawData[i] != null) {
                byte[] bytes = rawData[i].getBytes();
                result[i] = codec.decode(bytes, 0, bytes.length);
            } else {
                result[i] = null;
            }

        }
        return result;
    }

    private static SeekableChannelsProvider getChannelsProvider() {
        return new CachedChannelProvider(new LocalFSChannelProvider(), 1024);
    }


    private static PageReaderMaterializer[] getMaterializers(String filePath) throws IOException {
        ParquetFileReader pf = new ParquetFileReader(
                filePath, getChannelsProvider(), 0);
        System.out.println(pf.fileMetaData.getKey_value_metadata());
        ParquetMetadata pm = new ParquetMetadataConverter().fromParquetMetadata(pf.fileMetaData);
        RowGroupReader rg = pf.getRowGroup(0);

        MessageType schema = pf.getSchema();

        List<PageReaderMaterializer> readers = new ArrayList<>();
        for (ColumnDescriptor column : schema.getColumns()) {
            long start = System.nanoTime();
            System.out.println(column + ":");
            LogicalTypeAnnotation logicalTypeAnnotation = column.getPrimitiveType().getLogicalTypeAnnotation();
            Random random = new Random();
            if (logicalTypeAnnotation == null) {
                switch (column.getPrimitiveType().getPrimitiveTypeName()) {
                    case BOOLEAN:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], (byte) -1, random));
                        break;
                    case INT32:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Integer.MIN_VALUE + 1, random));
                        break;
                    case INT64:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Long.MIN_VALUE + 1, random));
                        break;
                    case DOUBLE:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Double.MIN_VALUE + 1, random));
                        break;
                    case FLOAT:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Float.MIN_VALUE + 1, random));
                        break;
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], null, random));
                        break;
                    case INT96:
                        throw new UncheckedDeephavenException("Unsupported type " + column.getPrimitiveType() + " for column " + Arrays.toString(column.getPath()));

                }
            } else {
                Optional<Object> result = logicalTypeAnnotation.accept(new MaterializedVisitor(rg, column, readers, random));
                if (result.isPresent() && result.get() instanceof Exception) {
                    throw new UncheckedDeephavenException("Unable to read column " + Arrays.toString(column.getPath()), (Throwable) result.get());
                }
            }
            System.out.println((System.nanoTime() - start) * 1.0 / 1000000000);

        }
        return readers.toArray(new PageReaderMaterializer[0]);
    }

    @SuppressWarnings("unused")
    private static List<String> binaryToString(Binary[] data) {
        List<String> result = new ArrayList<>(data.length);
        int i = 0;
        for (Binary datum : data) {
            result.add(datum == null ? null : datum.toStringUsingUTF8());
        }
        return result;
    }

    private static String toBooleanString(byte[] data) {
        return Arrays.toString(toBooleanArray(data));
    }

    private static Boolean[] toBooleanArray(byte[] data) {
        Boolean[] booleans = new Boolean[data.length];
        for (int i = 0; i < data.length; i++) {
            booleans[i] = data[i] == -1 ? null : data[i] == 1;
        }
        return booleans;
    }

    private static String cap(String string) {
        if (string.length() > 300) {
            return string.substring(0, 150) + "..." + string.substring(string.length() - 140);
        }
        return string;
    }


    private static void readRowGroup(RowGroupReader rg, Object nullValue, Consumer<Object> print, List<String> path) throws IOException {
        ColumnChunkReader cc = rg.getColumnChunk(path);
        Dictionary dictionary = cc.getDictionary();
        DictionaryAdapter dictionaryMapping = dictionary != null ? DictionaryAdapter.getAdapter(dictionary, nullValue) : null;
        int nullId = 0;
        if (dictionary != null) {
            nullId = dictionary.getMaxId() + 1;
        }
        try (ColumnChunkReader.ColumnPageReaderIterator it = cc.getPageIterator()) {
            boolean skip = false;
            while (it.hasNext()) {
                ColumnPageReader cr = it.next();
                System.out.println("cr.numRows() = " + cr.numRows());
                int numValues = cr.numValues();
                System.out.println("cr.numValues() = " + numValues);
                if (!skip) {
                    if (dictionaryMapping == null) {
                        print.accept(cr.materialize(nullValue));
                    } else {
                        IntBuffer intBuffer = IntBuffer.allocate((int) numValues);
                        IntBuffer offsets = cr.readKeyValues(intBuffer, nullId);
                        Object result = dictionaryMapping.createResult(numValues);
                        for (int i = 0; i < numValues; i++) {
                            dictionaryMapping.apply(result, i, intBuffer.get(i));
                        }
                        if (offsets != null) {
                            print.accept(new DataWithOffsets(offsets, result));
                        } else {
                            print.accept(result);
                        }
                    }
                }
               // skip = !skip;
            }
        } catch (Exception e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    private static PageReaderMaterializer getRowGroupColumnPageReaders(RowGroupReader rg, String columnName, Object nullValue, Random random) {
        List<ColumnPageReader> result = new ArrayList<>();
        ColumnChunkReader cc = rg.getColumnChunk(Collections.singletonList(columnName));
        try (ColumnChunkReader.ColumnPageReaderIterator it = cc.getPageIterator()) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        } catch (Exception e) {
            throw new UncheckedDeephavenException(e);
        }
        System.out.println(result.size() + " pages");
        return new PageReaderMaterializer(random, nullValue, result.toArray(new ColumnPageReader[0]));
    }

    static class PageReaderMaterializer {
        final Object nullValue;
        final ColumnPageReader[] readers;
        final Random random;

        PageReaderMaterializer(Random random, Object nullValue, ColumnPageReader[] readers) {
            this.nullValue = nullValue;
            this.readers = readers;
            this.random = random;
        }

        void readNext() throws IOException {
            readers[random.nextInt(readers.length)].materialize(nullValue);
        }
    }

    private static void testRandom(String filePath) throws IOException {
        PageReaderMaterializer[] materializers = getMaterializers(filePath);
        Random random = new Random();
        Thread[] threads = new Thread[10];
        for (int j = 0; j < threads.length; j++) {
            int finalJ = j;
            threads[j] = new Thread(() -> {
                while (true) {
                    long start = System.nanoTime();
                    for (int i = 0; i < 100; i++) {
                        try {
                            materializers[random.nextInt(materializers.length)].readNext();
                        } catch (IOException e) {
                            throw new UncheckedDeephavenException(e);
                        }
                    }
                    System.out.println(finalJ + ":" + (System.nanoTime() - start) * 0.001 / 1000);
                }
            });
            threads[j].start();
        }
    }

    private static class ReadRowGroupVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Object> {
        private final RowGroupReader rg;
        private final ColumnDescriptor column;

        ReadRowGroupVisitor(RowGroupReader rg, ColumnDescriptor column) {
            this.rg = rg;
            this.column = column;
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            try {
                Function<Object, String> toString = (data) -> Arrays.toString((String[]) data);
                Function<Object, String> finalToString = applyRepetitionLevel(rg, column, toString);
                readRowGroup(rg, null, (data) -> System.out.println(cap(finalToString.apply(data))), Arrays.asList(column.getPath()));
            } catch (IOException e) {
                return Optional.of(e);
            }
            return Optional.empty();
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + mapLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + listLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + enumLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + decimalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + dateLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + timeLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            Assert.eqTrue(timestampLogicalType.isAdjustedToUTC(), "tlta.isAdjustedToUTC()");

            Assert.eq(timestampLogicalType.getUnit(), "tlta.getUnit()", LogicalTypeAnnotation.TimeUnit.NANOS, "LogicalTypeAnnotation.TimeUnit.NANOS");
            try {
                Function<Object, String> toString = (data) -> {
                    Date[] a = Arrays.stream((long[]) data).mapToObj(t -> new Date(t / 1000000)).toArray(Date[]::new);
                    return Arrays.toString(a);
                };
                Function<Object, String> finalToString = applyRepetitionLevel(rg, column, toString);
                ;
                readRowGroup(rg, Long.MIN_VALUE + 1, (data) -> {
                    System.out.println(cap(finalToString.apply(data)));
                },Arrays.asList(column.getPath()));
            } catch (IOException e) {
                return Optional.of(e);
            }
            return Optional.empty();
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            try {
                Function<Object, String> toString = null;
                Object nullValue = null;
                if (intLogicalType.isSigned()) {
                    switch (intLogicalType.getBitWidth()) {
                        case 32:
                            nullValue = Integer.MIN_VALUE + 1;
                            toString = (data) -> Arrays.toString((int[]) data);
                            break;
                        case 16:
                            nullValue = Short.MIN_VALUE + 1;
                            toString = (data) -> Arrays.toString((int[]) data);
                            break;
                        case 8:
                            nullValue = Byte.MIN_VALUE + 1;
                            toString = (data) -> Arrays.toString((int[]) data);
                            break;
                        case 64:
                            toString = (data) -> Arrays.toString((long[]) data);
                            nullValue = Long.MIN_VALUE + 1;
                            break;
                        default:
                            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + intLogicalType));
                    }
                } else if (intLogicalType.getBitWidth() == 16) {
                    nullValue = Short.MIN_VALUE + 1;
                    toString = (data) -> Arrays.toString((int[]) data);
                } else {
                    return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + intLogicalType));
                }
                toString = applyRepetitionLevel(rg, column, toString);
                Function<Object, String> finalToString = toString;
                readRowGroup(rg, nullValue, (data) -> System.out.println(cap(finalToString.apply(data))),Arrays.asList(column.getPath()));
            } catch (IOException e) {
                return Optional.of(e);
            }
            return Optional.empty();
        }


        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + jsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + bsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + intervalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + mapKeyValueLogicalType));
        }
    }

    private static class MaterializedVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Object> {
        private final RowGroupReader rg;
        private final ColumnDescriptor column;
        private final List<PageReaderMaterializer> readers;
        private final Random random;

        MaterializedVisitor(RowGroupReader rg, ColumnDescriptor column, List<PageReaderMaterializer> readers, Random random) {
            this.rg = rg;
            this.column = column;
            this.readers = readers;
            this.random = random;
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], null, random));
            return Optional.empty();
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + mapLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + listLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + enumLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + decimalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + dateLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + timeLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            Assert.eqTrue(timestampLogicalType.isAdjustedToUTC(), "tlta.isAdjustedToUTC()");

            Assert.eq(timestampLogicalType.getUnit(), "tlta.getUnit()", LogicalTypeAnnotation.TimeUnit.NANOS, "LogicalTypeAnnotation.TimeUnit.NANOS");
            readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Long.MIN_VALUE + 1, random));

            return Optional.empty();
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            if (intLogicalType.isSigned()) {
                switch (intLogicalType.getBitWidth()) {
                    case 32:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Integer.MIN_VALUE + 1, random));
                        break;
                    case 64:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Long.MIN_VALUE + 1, random));
                        break;
                    case 16:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Short.MIN_VALUE - 1, random));
                        break;
                    case 8:
                        readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Byte.MIN_VALUE - 1, random));
                        break;
                    default:
                        return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + intLogicalType));
                }
            } else if (intLogicalType.getBitWidth() == 16) {
                readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Character.MIN_VALUE - 1, random));
            } else {
                return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + intLogicalType));
            }

            return Optional.empty();
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + jsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + bsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + intervalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
            return Optional.of(new UncheckedDeephavenException("Unsupported logical type " + mapKeyValueLogicalType));
        }
    }

    static String convertDataWithOffsets(DataWithOffsets dwo, Function<Object, String> print) {
        int prevOffset = 0;
        Class<?> component = dwo.materializeResult.getClass().getComponentType();
        StringBuilder result = new StringBuilder("[");
        while (dwo.offsets.hasRemaining()) {
            int endOffset = dwo.offsets.get();
            if (endOffset == -1) {
                result.append(" null ");
            } else {
                Object tmp = Array.newInstance(component, endOffset - prevOffset);
                System.arraycopy(dwo.materializeResult, prevOffset, tmp, 0, endOffset - prevOffset);
                result.append(print.apply(tmp));
                prevOffset = endOffset;
            }
        }
        return cap(result.toString());
    }

    private static Function<Object, String> applyRepetitionLevel(RowGroupReader rg, ColumnDescriptor column, Function<Object, String> toString) {
        if (rg.getColumnChunk(Arrays.asList(column.getPath())).getMaxRl() > 0) {
            Function<Object, String> finalToString = toString;
            toString = (data) -> convertDataWithOffsets((DataWithOffsets) data, finalToString);
        }
        return toString;
    }

}

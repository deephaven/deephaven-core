package io.deephaven.db.v2.parquet;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.parquet.*;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.utils.CachedChannelProvider;
import io.deephaven.parquet.utils.LocalFSChannelProvider;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.IntBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

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
                            String codecName = keyValueMetaData.get(ParquetTableWriter._CODEC_NAME_PREFIX_ + column.getPath()[0]);
                            String codecParams = keyValueMetaData.get(ParquetTableWriter._CODEC_ARGS_PREFIX_ + column.getPath()[0]);
                            ObjectCodec<Object> codec = CodecCache.DEFAULT.getCodec(codecName, codecParams);
                            toString = (data) -> Arrays.toString((Object[]) convertArray((Binary[]) data, codec));
                            break;
                        case INT96:
                            throw new RuntimeException("Unsupported type " + column.getPrimitiveType() + " for column " + Arrays.toString(column.getPath()));
                    }
                    toString = applyRepetitionLevel(rg, column, toString);
                    Function<Object, String> finalToString = toString;
                    readRowGroup(rg, nullValue, (data) -> System.out.println(cap(finalToString.apply(data))), Arrays.asList(column.getPath()));
                } else {
                    Optional<Object> result = logicalTypeAnnotation.accept(new ReadRowGroupVisitor(rg, column));
                    if (result.isPresent() && result.get() instanceof Exception) {
                        throw new RuntimeException("Unable to read column " + Arrays.toString(column.getPath()), (Throwable) result.get());
                    }
                }
                System.out.println((System.nanoTime() - start) * 1.0 / 1000000000);
            }
        }
    }

    public static void readParquetSchema(
            final String filePath, final BiConsumer<String, Class<?>> colDefConsumer
    ) throws IOException {
        final ParquetFileReader pf = new ParquetFileReader(
                filePath, getChannelsProvider(), 0);
        final MessageType schema = pf.getSchema();
        final LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>> visitor = new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>>() {
            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                return Optional.of(String.class);
            }
            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
                if (timestampLogicalType.isAdjustedToUTC() && timestampLogicalType.getUnit().equals(LogicalTypeAnnotation.TimeUnit.NANOS)) {
                    return Optional.of(io.deephaven.db.tables.utils.DBDateTime.class);
                }
                return Optional.empty();
            }
        };
        int c = 0;
        for (ColumnDescriptor column : schema.getColumns()) {
            LogicalTypeAnnotation logicalTypeAnnotation = column.getPrimitiveType().getLogicalTypeAnnotation();
            final String colName = schema.getFieldName(c++);
            if (logicalTypeAnnotation == null) {
                switch (column.getPrimitiveType().getPrimitiveTypeName()) {
                    case BOOLEAN:
                        colDefConsumer.accept(colName, Boolean.class);
                        break;
                    case INT32:
                        colDefConsumer.accept(colName, int.class);
                        break;
                    case INT64:
                        colDefConsumer.accept(colName, long.class);
                        break;
                    case DOUBLE:
                        colDefConsumer.accept(colName, double.class);
                        break;
                    case FLOAT:
                        colDefConsumer.accept(colName, float.class);
                        break;
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        // Do not directly map to String when the logical annotation is not present.
                    default:
                        throw new RuntimeException("Unsupported type " + column.getPrimitiveType() + " for column " + Arrays.toString(column.getPath()));
                }
            } else {
                final Optional<Class<?>> optionalClass = logicalTypeAnnotation.accept(visitor);
                if (!optionalClass.isPresent()) {
                    throw new RuntimeException("Unable to read column " + Arrays.toString(column.getPath()) + ", no mappeable logical type annotation found.");
                }
                colDefConsumer.accept(colName, optionalClass.get());
            }
        }
    }

    private static Object[] convertArray(Binary[] rawData, ObjectCodec codec) {
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
                        throw new RuntimeException("Unsupported type " + column.getPrimitiveType() + " for column " + Arrays.toString(column.getPath()));

                }
            } else {
                Optional<Object> result = logicalTypeAnnotation.accept(new MaterializedVisitor(rg, column, readers, random));
                if (result.isPresent() && result.get() instanceof Exception) {
                    throw new RuntimeException("Unable to read column " + Arrays.toString(column.getPath()), (Throwable) result.get());
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
        int maxRl = cc.getMaxRl();
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
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
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
                            throw new RuntimeException(e);
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
            return Optional.of(new RuntimeException("Unsupported logical type " + mapLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + listLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + enumLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + decimalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + dateLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + timeLogicalType));
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
                            return Optional.of(new RuntimeException("Unsupported logical type " + intLogicalType));
                    }
                } else if (intLogicalType.getBitWidth() == 16) {
                    nullValue = Short.MIN_VALUE + 1;
                    toString = (data) -> Arrays.toString((int[]) data);
                } else {
                    return Optional.of(new RuntimeException("Unsupported logical type " + intLogicalType));
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
            return Optional.of(new RuntimeException("Unsupported logical type " + jsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + bsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + intervalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + mapKeyValueLogicalType));
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
            return Optional.of(new RuntimeException("Unsupported logical type " + mapLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + listLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + enumLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + decimalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + dateLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + timeLogicalType));
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
                        return Optional.of(new RuntimeException("Unsupported logical type " + intLogicalType));
                }
            } else if (intLogicalType.getBitWidth() == 16) {
                readers.add(getRowGroupColumnPageReaders(rg, column.getPath()[0], Character.MIN_VALUE - 1, random));
            } else {
                return Optional.of(new RuntimeException("Unsupported logical type " + intLogicalType));
            }

            return Optional.empty();
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + jsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + bsonLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + intervalLogicalType));
        }

        @Override
        public Optional<Object> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
            return Optional.of(new RuntimeException("Unsupported logical type " + mapKeyValueLogicalType));
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

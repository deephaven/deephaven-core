package io.deephaven.csv;

import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.*;
import io.deephaven.time.DateTime;
import io.deephaven.time.TimeZone;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.*;

/**
 * A specification object for parsing a CSV, or CSV-like, structure into a {@link NewTable}.
 */
@Immutable
@BuildableStyle
public abstract class CsvSpecs {

    public interface Builder {
        Builder header(TableHeader header);

        Builder addHeaders(String... headers);

        Builder addAllHeaders(Iterable<String> headers);

        Builder putHeaderForIndex(int index, String header);

        Builder putParserForName(String columnName, Parser<?> parser);

        Builder putParserForIndex(int index, Parser<?> parser);

        Builder nullValueLiteral(String nullValueLiteral);

        Builder putNullValueLiteralForName(String columnName, String nullValueLiteral);

        Builder putNullValueLiteralForIndex(int index, String nullValueLiteral);

        Builder inference(InferenceSpecs inferenceSpecs);

        Builder hasHeaderRow(boolean hasHeaderRow);

        Builder delimiter(char delimiter);

        Builder quote(char quote);

        Builder ignoreSurroundingSpaces(boolean ignoreSurroundingSpaces);

        Builder trim(boolean trim);

        Builder concurrent(boolean async);

        CsvSpecs build();
    }

    /**
     * Creates a builder for {@link CsvSpecs}.
     *
     * @return the builder
     */
    public static Builder builder() {
        return ImmutableCsvSpecs.builder();
    }

    /**
     * A comma-separated-value delimited format.
     *
     * <p>
     * Equivalent to {@code builder().build()}.
     *
     * @return the spec
     */
    public static CsvSpecs csv() {
        return builder().build();
    }

    /**
     * A tab-separated-value delimited format.
     *
     * <p>
     * Equivalent to {@code builder().delimiter('\t').build()}.
     *
     * @return the spec
     */
    public static CsvSpecs tsv() {
        return builder().delimiter('\t').build();
    }

    /**
     * A header-less, CSV format.
     *
     * <p>
     * Equivalent to {@code builder().hasHeaderRow(false).build()}.
     *
     * @return the spec
     */
    public static CsvSpecs headerless() {
        return builder().hasHeaderRow(false).build();
    }

    /**
     * A header-less, CSV format, with the user providing the {@code header}.
     *
     * <p>
     * Equivalent to {@code builder().hasHeaderRow(false).header(header).build()}.
     *
     * @param header the header to use
     * @return the spec
     */
    public static CsvSpecs headerless(TableHeader header) {
        return builder().hasHeaderRow(false).header(header).build();
    }

    public static CsvSpecs fromLegacyFormat(String format) {
        if (format == null) {
            return CsvSpecs.csv();
        } else if (format.length() == 1) {
            return CsvSpecs.builder().delimiter(format.charAt(0)).build();
        } else if ("TRIM".equals(format)) {
            return CsvSpecs.builder().trim(true).build();
        } else if ("DEFAULT".equals(format)) {
            return CsvSpecs.builder().ignoreSurroundingSpaces(false).build();
        } else if ("TDF".equals(format)) {
            return CsvSpecs.tsv();
        }
        return null;
    }

    /**
     * A header, when specified, hints at the parser to use.
     *
     * <p>
     * To be even more explicit, callers may also use {@link #parserForName()} or {@link #parserForIndex()}.
     *
     * @return the table header.
     */
    public abstract Optional<TableHeader> header();

    /**
     * A list of column header names that, when specified, overrides the column names that would otherwise be used.
     */
    public abstract List<String> headers();

    /**
     * Header overrides, where the keys are 1-based column indices. Specifying a column header overrides the header that
     * would otherwise be used for that specific column.
     */
    public abstract Map<Integer, String> headerForIndex();

    /**
     * The parsers, where the keys are column names. Specifying a parser for a column forgoes inference for that column.
     *
     * @return the parsers.
     */
    public abstract Map<String, Parser<?>> parserForName();

    /**
     * The parsers, where the keys are 1-based column indices. Specifying a parser for a column forgoes inference for
     * that column.
     *
     * @return the parsers.
     */
    public abstract Map<Integer, Parser<?>> parserForIndex();

    /**
     * The null value literal that is used when it is not overridden for any particular column.
     */
    @Default
    public String nullValueLiteral() {
        return "";
    }

    /**
     * The null value literals, where the keys are column names. Specifying a null value literal for a column overrides
     * the default null value literal, which is the empty string.
     *
     * @return the null value literals
     */
    public abstract Map<String, String> nullValueLiteralForName();

    /**
     * The null value literals, where the keys are 1-based column indices. Specifying a null value literal for a column
     * overrides the default null value literal, which is the empty string.
     *
     * @return the null value literals
     */
    public abstract Map<Integer, String> nullValueLiteralForIndex();

    /**
     * The inference specifications.
     *
     * <p>
     * By default, is {@link InferenceSpecs#standard()}.
     *
     * @return the inference specifications
     */
    @Default
    public InferenceSpecs inference() {
        return InferenceSpecs.standard();
    }

    /**
     * The header row flag. If {@code true}, the column names of the output table will be inferred from the first row of
     * the table. If {@code false}, the column names will be numbered numerically in the format "Column%d" with a
     * 1-based index.
     *
     * <p>
     * Note: if {@link #header()} is specified, it takes precedence over the column names that will be used.
     *
     * <p>
     * By default is {@code true}.
     *
     * @return the header row flag
     */
    @Default
    public boolean hasHeaderRow() {
        return true;
    }

    /**
     * The delimiter character.
     *
     * <p>
     * By default is ','.
     *
     * @return the delimiter character
     */
    @Default
    public char delimiter() {
        return ',';
    }

    /**
     * The quote character.
     *
     * <p>
     * By default is '"'.
     *
     * @return the quote character
     */
    @Default
    public char quote() {
        return '"';
    }


    /**
     * The ignore surrounding spaces flag, whether to trim leading and trailing blanks from non-quoted values.
     *
     * <p>
     * By default is {@code true}
     *
     * @return the ignore surrounding spaces flag
     */
    @Default
    public boolean ignoreSurroundingSpaces() {
        return true;
    }

    /**
     * The trim flag, whether to trim leading and trailing blanks from inside quoted values.
     *
     * <p>
     * By default is {@code false}.
     *
     * @return the trim flag
     */
    @Default
    public boolean trim() {
        return false;
    }

    /**
     * The character set.
     *
     * <p>
     * By default, is UTF-8.
     *
     * @return the character set.
     */
    @Default
    public Charset charset() {
        return StandardCharsets.UTF_8;
    }

    /**
     * Should the CSVReader run its processing steps concurrently on multiple threads for better performance.
     * 
     * @return the concurrent flag
     */
    @Default
    public boolean concurrent() {
        return true;
    }

    /**
     * Parses {@code string} according to the specifications of {@code this}.
     *
     * @param string the string
     * @return the new table
     * @throws CsvReaderException if any sort of failure occurs.
     */
    public final Table parse(String string) throws CsvReaderException {
        final StringReader reader = new StringReader(string);
        final ReaderInputStream inputStream = new ReaderInputStream(reader, StandardCharsets.UTF_8);
        return parse(inputStream);
    }

    /**
     * Parses {@code stream} according to the specifications of {@code this}. The {@code stream} will be closed upon
     * return.
     *
     * <p>
     * Note: this implementation will buffer the {@code stream} internally.
     *
     * @param stream the stream
     * @return the new table
     * @throws CsvReaderException if any sort of failure occurs.
     */
    public final Table parse(InputStream stream) throws CsvReaderException {
        final CsvReader csvReader = configureCsvReader();
        final CsvReader.Result result = csvReader.read(stream, makeMySinkFactory());

        final String[] columnNames = result.columnNames();
        final Sink<?>[] sinks = result.columns();
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        long maxSize = 0;
        for (int ii = 0; ii < columnNames.length; ++ii) {
            final String columnName = columnNames[ii];
            final MySinkBase<?, ?> sink = (MySinkBase<?, ?>) sinks[ii];
            maxSize = Math.max(maxSize, sink.resultSize());
            columns.put(columnName, sink.result());
        }
        final TableDefinition tableDef = TableDefinition.inferFrom(columns);
        final TrackingRowSet rowSet = RowSetFactory.flat(maxSize).toTracking();
        return InMemoryTable.from(tableDef, rowSet, columns);
    }

    private CsvReader configureCsvReader() {
        final CsvReader csvReader = new CsvReader();

        csvReader.setConcurrent(concurrent());
        csvReader.setIgnoreSurroundingSpaces(ignoreSurroundingSpaces());
        csvReader.setTrim(trim());
        csvReader.setHasHeaders(hasHeaderRow());
        csvReader.setquoteChar(quote());
        csvReader.setFieldDelimiter(delimiter());
        csvReader.setParsers(inference().parsers());

        for (Map.Entry<String, Parser<?>> entry : parserForName().entrySet()) {
            csvReader.setParserFor(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Integer, Parser<?>> entry : parserForIndex().entrySet()) {
            csvReader.setParserFor(entry.getKey(), entry.getValue());
        }


        csvReader.setNullValueLiteral(nullValueLiteral());
        for (Map.Entry<String, String> entry : nullValueLiteralForName().entrySet()) {
            csvReader.setNullValueLiteralFor(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Integer, String> entry : nullValueLiteralForIndex().entrySet()) {
            csvReader.setNullValueLiteralFor(entry.getKey(), entry.getValue());
        }

        if (header().isPresent()) {
            final List<String> headers = new ArrayList<>();
            for (ColumnHeader<?> ch : header().get()) {
                headers.add(ch.name());
                csvReader.setParserFor(ch.name(), typeToParser(ch.componentType()));
            }
            csvReader.setHeaders(headers);
        }

        csvReader.setNullParser(inference().nullParser());

        csvReader.setCustomTimeZoneParser(new TimeZoneParser());

        csvReader.setHeaderLegalizer(names -> NameValidator.legalizeColumnNames(names,
                s -> s.replaceAll("[- ]", "_"), true));
        csvReader.setHeaderValidator(NameValidator::isValidColumnName);

        return csvReader;
    }

    private static abstract class MySinkBase<TYPE, TARRAY> implements Sink<TARRAY> {
        protected final ArrayBackedColumnSource<TYPE> result;
        protected long resultSize;
        protected final WritableColumnSource<?> reinterpreted;
        protected final ChunkWrapInvoker<TARRAY, Chunk<? extends Values>> chunkWrapInvoker;

        public MySinkBase(ArrayBackedColumnSource<TYPE> result, Class<?> interpClass,
                ChunkWrapInvoker<TARRAY, Chunk<? extends Values>> chunkWrapInvoker) {
            this.result = result;
            this.resultSize = 0;
            if (interpClass != null) {
                reinterpreted = (WritableColumnSource<?>) result.reinterpret(interpClass);
            } else {
                reinterpreted = result;
            }
            this.chunkWrapInvoker = chunkWrapInvoker;
        }

        @Override
        public final void write(final TARRAY src, final boolean[] isNull, final long destBegin, final long destEnd,
                boolean appending_unused) {
            if (destBegin == destEnd) {
                return;
            }
            final int size = Math.toIntExact(destEnd - destBegin);
            nullFlagsToValues(src, isNull, size);
            reinterpreted.ensureCapacity(destEnd);
            resultSize = Math.max(resultSize, destEnd);
            try (final ChunkSink.FillFromContext context = reinterpreted.makeFillFromContext(size);
                    final RowSequence range = RowSequenceFactory.forRange(destBegin, destEnd - 1)) {
                Chunk<? extends Values> chunk = chunkWrapInvoker.apply(src, 0, size);
                reinterpreted.fillFromChunk(context, chunk, range);
            }
        }

        protected abstract void nullFlagsToValues(final TARRAY values, final boolean[] isNull, final int size);

        public ArrayBackedColumnSource<TYPE> result() {
            return result;
        }

        public long resultSize() {
            return resultSize;
        }

        protected interface ChunkWrapInvoker<TARRAY, TRESULT> {
            TRESULT apply(final TARRAY data, final int offset, final int capacity);
        }
    }

    private static abstract class MySourceAndSinkBase<TYPE, TARRAY> extends MySinkBase<TYPE, TARRAY>
            implements Source<TARRAY>, Sink<TARRAY> {
        private final ChunkWrapInvoker<TARRAY, WritableChunk<? super Values>> writableChunkWrapInvoker;

        public MySourceAndSinkBase(ArrayBackedColumnSource<TYPE> result, Class<?> interpClass,
                ChunkWrapInvoker<TARRAY, Chunk<? extends Values>> chunkWrapInvoker,
                ChunkWrapInvoker<TARRAY, WritableChunk<? super Values>> writeableChunkWrapInvoker) {
            super(result, interpClass, chunkWrapInvoker);
            this.writableChunkWrapInvoker = writeableChunkWrapInvoker;
        }

        @Override
        public final void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            if (srcBegin == srcEnd) {
                return;
            }
            final int size = Math.toIntExact(srcEnd - srcBegin);
            try (final ChunkSink.FillContext context = reinterpreted.makeFillContext(size);
                    final RowSequence range = RowSequenceFactory.forRange(srcBegin, srcEnd - 1)) {
                WritableChunk<? super Values> chunk = writableChunkWrapInvoker.apply(dest, 0, size);
                reinterpreted.fillChunk(context, chunk, range);
            }
            valuesToNullFlags(dest, isNull, size);
        }

        protected abstract void valuesToNullFlags(final TARRAY values, final boolean[] isNull, final int size);
    }

    private static final class MyCharSink extends MySinkBase<Character, char[]> {
        public MyCharSink() {
            super(new CharacterArraySource(), null, CharChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final char[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_CHAR;
                }
            }
        }
    }

    private static final class MyBooleanAsByteSink extends MySinkBase<Boolean, byte[]> {
        public MyBooleanAsByteSink() {
            super(new BooleanArraySource(), byte.class, ByteChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = BooleanUtils.NULL_BOOLEAN_AS_BYTE;
                }
            }
        }
    }

    private static final class MyByteSink extends MySourceAndSinkBase<Byte, byte[]> {
        public MyByteSink() {
            super(new ByteArraySource(), null, ByteChunk::chunkWrap, WritableByteChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_BYTE;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_BYTE;
            }
        }
    }

    private static final class MyShortSink extends MySourceAndSinkBase<Short, short[]> {
        public MyShortSink() {
            super(new ShortArraySource(), null, ShortChunk::chunkWrap, WritableShortChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final short[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_SHORT;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final short[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_SHORT;
            }
        }
    }

    private static final class MyIntSink extends MySourceAndSinkBase<Integer, int[]> {
        public MyIntSink() {
            super(new IntegerArraySource(), null, IntChunk::chunkWrap, WritableIntChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final int[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_INT;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final int[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_INT;
            }
        }
    }

    private static final class MyLongSink extends MySourceAndSinkBase<Long, long[]> {
        public MyLongSink() {
            super(new LongArraySource(), null, LongChunk::chunkWrap, WritableLongChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_LONG;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_LONG;
            }
        }

    }

    private static final class MyFloatSink extends MySinkBase<Float, float[]> {
        public MyFloatSink() {
            super(new FloatArraySource(), null, FloatChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final float[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_FLOAT;
                }
            }
        }
    }

    private static final class MyDoubleSink extends MySinkBase<Double, double[]> {
        public MyDoubleSink() {
            super(new DoubleArraySource(), null, DoubleChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final double[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_DOUBLE;
                }
            }
        }
    }

    private static final class MyStringSink extends MySinkBase<String, String[]> {
        public MyStringSink() {
            super(new ObjectArraySource<>(String.class), null, ObjectChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final String[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = null;
                }
            }
        }
    }

    private static final class MyDateTimeAsLongSink extends MySinkBase<DateTime, long[]> {
        public MyDateTimeAsLongSink() {
            super(new DateTimeArraySource(), long.class, LongChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_LONG;
                }
            }
        }
    }

    private static SinkFactory makeMySinkFactory() {
        return SinkFactory.of(
                MyByteSink::new, QueryConstants.NULL_BYTE_BOXED,
                MyShortSink::new, QueryConstants.NULL_SHORT_BOXED,
                MyIntSink::new, QueryConstants.NULL_INT_BOXED,
                MyLongSink::new, QueryConstants.NULL_LONG_BOXED,
                MyFloatSink::new, QueryConstants.NULL_FLOAT_BOXED,
                MyDoubleSink::new, QueryConstants.NULL_DOUBLE_BOXED,
                MyBooleanAsByteSink::new,
                MyCharSink::new, QueryConstants.NULL_CHAR,
                MyStringSink::new, null,
                MyDateTimeAsLongSink::new, QueryConstants.NULL_LONG,
                MyDateTimeAsLongSink::new, QueryConstants.NULL_LONG);
    }

    private static Parser<?> typeToParser(Type<?> type) {
        return type.walk(new MyVisitor()).out;
    }

    private static final class MyVisitor implements Type.Visitor, PrimitiveType.Visitor, GenericType.Visitor {
        private Parser<?> out;

        @Override
        public void visit(PrimitiveType<?> primitiveType) {
            primitiveType.walk((PrimitiveType.Visitor) this);
        }

        @Override
        public void visit(GenericType<?> genericType) {
            genericType.walk((GenericType.Visitor) this);
        }

        @Override
        public void visit(BooleanType booleanType) {
            out = Parsers.BOOLEAN;
        }

        @Override
        public void visit(ByteType byteType) {
            out = Parsers.BYTE;
        }

        @Override
        public void visit(CharType charType) {
            out = Parsers.CHAR;
        }

        @Override
        public void visit(ShortType shortType) {
            out = Parsers.SHORT;
        }

        @Override
        public void visit(IntType intType) {
            out = Parsers.INT;
        }

        @Override
        public void visit(LongType longType) {
            out = Parsers.LONG;
        }

        @Override
        public void visit(FloatType floatType) {
            out = Parsers.FLOAT_FAST;
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = Parsers.DOUBLE;
        }

        @Override
        public void visit(StringType stringType) {
            out = Parsers.STRING;
        }

        @Override
        public void visit(InstantType instantType) {
            throw new RuntimeException("Logic error: there is no Parser for " + instantType);
        }

        @Override
        public void visit(ArrayType<?, ?> arrayType) {
            throw new RuntimeException("Logic error: there is no Parser for " + arrayType);
        }

        @Override
        public void visit(CustomType<?> customType) {
            throw new RuntimeException("Logic error: there is no Parser for " + customType);
        }
    }

    /**
     * A class that aids in Deephaven TimeZone parsing. In particular it memorizes the set of known Deephaven
     * DateTimeZones and keeps them in a hashmap for fast lookup. It also remembers the last timezone looked up for even
     * faster access. It is used as a callback for the Tokenizer class.
     */
    private static final class TimeZoneParser implements Tokenizer.CustomTimeZoneParser {
        private static final String DEEPHAVEN_TZ_PREFIX = "TZ_";
        private static final int MAX_DEEPHAVEN_TZ_LENGTH = 3;

        private final TIntObjectHashMap<ZoneId> zoneIdMap = new TIntObjectHashMap<>();

        private int lastTzKey = -1;
        private ZoneId lastZoneId = null;

        public TimeZoneParser() {
            for (TimeZone zone : TimeZone.values()) {
                final String zname = zone.name();
                if (!zname.startsWith(DEEPHAVEN_TZ_PREFIX)) {
                    throw new RuntimeException("Logic error: unexpected enum in DBTimeZone: " + zname);
                }
                final String zSuffix = zname.substring(DEEPHAVEN_TZ_PREFIX.length());
                final int zlen = zSuffix.length();
                if (zlen > MAX_DEEPHAVEN_TZ_LENGTH) {
                    throw new RuntimeException("Logic error: unexpectedly-long enum in DBTimeZone: " + zname);
                }
                final byte[] data = new byte[zlen];
                for (int ii = 0; ii < zlen; ++ii) {
                    final char ch = zSuffix.charAt(ii);
                    if (!RangeTests.isUpper(ch)) {
                        throw new RuntimeException("Logic error: unexpected character in DBTimeZone name: " + zname);
                    }
                    data[ii] = (byte) ch;
                }
                final ByteSlice bs = new ByteSlice(data, 0, data.length);
                final int tzKey = tryParseTzKey(bs);
                if (tzKey < 0) {
                    throw new RuntimeException("Logic error: can't parse DBTimeZone as key: " + zname);
                }
                final ZoneId zoneId = zone.getTimeZone().toTimeZone().toZoneId();
                zoneIdMap.put(tzKey, zoneId);
            }
        }

        @Override
        public boolean tryParse(ByteSlice bs, MutableObject<ZoneId> zoneId, MutableLong offsetSeconds) {
            if (bs.size() == 0 || bs.front() != ' ') {
                return false;
            }
            final int savedBegin = bs.begin();
            bs.setBegin(bs.begin() + 1);
            final int tzKey = tryParseTzKey(bs);
            if (tzKey < 0) {
                bs.setBegin(savedBegin);
                return false;
            }
            if (tzKey != lastTzKey) {
                final ZoneId res = zoneIdMap.get(tzKey);
                if (res == null) {
                    bs.setBegin(savedBegin);
                    return false;
                }
                lastTzKey = tzKey;
                lastZoneId = res;
            }
            zoneId.setValue(lastZoneId);
            offsetSeconds.setValue(0);
            return true;
        }

        /**
         * Take up to three uppercase characters from a TimeZone string and pack them into an integer.
         *
         * @param bs A ByteSlice holding the timezone key.
         * @return The characters packed into an int, or -1 if there are too many or too few characters, or if the
         *         characters are not uppercase ASCII.
         */
        private static int tryParseTzKey(final ByteSlice bs) {
            int res = 0;
            int current;
            for (current = bs.begin(); current != bs.end(); ++current) {
                if (current - bs.begin() > MAX_DEEPHAVEN_TZ_LENGTH) {
                    return -1;
                }
                final char ch = RangeTests.toUpper((char) bs.data()[current]);
                if (!RangeTests.isUpper(ch)) {
                    // If it's some nonalphabetic character
                    break;
                }
                res = res * 26 + (ch - 'A');
            }
            if (current - bs.begin() == 0) {
                return -1;
            }
            bs.setBegin(current);
            return res;
        }
    }
}

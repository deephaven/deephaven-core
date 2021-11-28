package io.deephaven.csv;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.Type;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A specification object for parsing a CSV, or CSV-like, structure into a {@link NewTable}.
 */
@Immutable
@BuildableStyle
public abstract class CsvSpecs {

    public interface Builder {

        Builder header(TableHeader header);

        Builder putParsers(String columnName, Parser<?> parser);

        Builder inference(InferenceSpecs inferenceSpecs);

        Builder hasHeaderRow(boolean hasHeaderRow);

        Builder delimiter(char delimiter);

        Builder quote(char quote);

        Builder ignoreSurroundingSpaces(boolean ignoreSurroundingSpaces);

        Builder trim(boolean trim);

        Builder charset(Charset charset);

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
     * To be even more explicit, callers may also use {@link #parsers()}.
     *
     * @return the table header
     */
    public abstract Optional<TableHeader> header();

    /**
     * The parsers, where the keys are column names. Specifying a parser for a column forgoes inference for that column.
     *
     * @return the parsers
     */
    public abstract Map<String, Parser<?>> parsers();

    /**
     * The inference specifications.
     *
     * <p>
     * By default, is {@link InferenceSpecs#standardTimes()}.
     *
     * @return the inference specifications
     */
    @Default
    public InferenceSpecs inference() {
        return InferenceSpecs.standardTimes();
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

    private CSVFormat format() {
        return CSVFormat.DEFAULT
                .withIgnoreSurroundingSpaces(ignoreSurroundingSpaces())
                .withDelimiter(delimiter())
                .withQuote(quote())
                .withTrim(trim());
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
     * @throws IOException if an I/O exception occurs
     */
    public final NewTable parse(InputStream stream) throws IOException {
        return parse(new InputStreamReader(stream, charset()));
    }

    /**
     * Parses {@code reader} according to the specifications of {@code this}. The {@code reader} will be closed upon
     * return.
     *
     * <p>
     * Note: this implementation will buffer the {@code reader} internally.
     *
     * @param reader the reader
     * @return the new table
     * @throws IOException if an I/O exception occurs
     */
    public final NewTable parse(Reader reader) throws IOException {
        try (
                final CSVParser csvParser = format().parse(reader)) {
            final List<CSVRecord> records = csvParser.getRecords();
            if (hasHeaderRow() && records.isEmpty()) {
                throw new IllegalStateException("Expected header row, none found");
            }
            final List<CSVRecord> dataRecords = hasHeaderRow() ? records.subList(1, records.size()) : records;
            if (!header().isPresent() && dataRecords.isEmpty()) {
                throw new IllegalStateException("Unable to infer types with no TableHeader and no data");
            }
            final int numColumns = records.get(0).size();
            if (numColumns == 0) {
                throw new IllegalStateException("Unable to parse an empty CSV");
            }
            final Iterable<String> columnNames;
            if (header().isPresent()) {
                columnNames = header().get().columnNames();
            } else if (hasHeaderRow()) {
                columnNames = legalizeColumnNames(records.get(0));
            } else {
                columnNames = IntStream
                        .range(0, numColumns)
                        .mapToObj(i -> String.format("Column%d", i + 1))
                        .collect(Collectors.toList());
            }

            final NewTable.Builder table = NewTable.builder();
            int columnIndex = 0;
            int size = -1;
            for (String columnName : columnNames) {
                final Parser<?> parser = parser(columnName, columnIndex, dataRecords);
                final Array<?> array = buildArray(getColumn(columnIndex, dataRecords), parser, dataRecords.size());
                if (size == -1) {
                    size = array.size();
                }
                table.putColumns(columnName, array);
                ++columnIndex;
            }
            return table.size(size).build();
        }
    }

    private static List<String> legalizeColumnNames(CSVRecord record) {
        final Set<String> taken = new HashSet<>(record.size());
        final List<String> out = new ArrayList<>(record.size());
        for (String name : record) {
            out.add(NameValidator.legalizeColumnName(name, (s) -> s.replaceAll("[- ]", "_"), taken));
        }
        return out;
    }

    private static Type<?> type(TableHeader header, String columnName) {
        final Type<?> type = header.getHeader(columnName);
        if (type != null) {
            return type;
        }
        throw new IllegalArgumentException(String.format(
                "When specifying a header, all columns must be accounted for. Missing type for column name '%s'",
                columnName));
    }

    private Parser<?> parser(String columnName, int columnIndex, List<CSVRecord> dataRecords) {
        final Type<?> type = header().map(header -> type(header, columnName)).orElse(null);

        // 1. An explicit parser if set
        final Parser<?> explicit = parsers().get(columnName);
        if (explicit != null) {
            if (type != null && !type.equals(explicit.type())) {
                throw new IllegalArgumentException("Explicit parser type and column header type do not match");
            }
            return explicit;
        }

        final InferenceSpecs inference;
        if (type != null) {
            // 2. Guided inference
            inference = inference().limitToType(type);
        } else {
            // 3. Original inference
            inference = inference();
        }

        final Optional<Parser<?>> p = inference.infer(getColumn(columnIndex, dataRecords));
        if (!p.isPresent()) {
            throw new IllegalStateException(
                    String.format("Unable to infer type for column '%s'", columnName));
        }
        return p.get();
    }

    private static <T> Array<T> buildArray(Iterator<String> it, Parser<T> parser, int size) {
        final ArrayBuilder<T, ?, ?> builder = Array.builder(parser.type(), size);
        while (it.hasNext()) {
            final T item = parser.parse(it.next());
            builder.add(item);
        }
        return builder.build();
    }

    private static Iterator<String> getColumn(int index, Iterable<CSVRecord> records) {
        return new CsvColumnIterator(index, records.iterator());
    }

    private static class CsvColumnIterator implements Iterator<String> {
        private final int index;
        private final Iterator<CSVRecord> it;

        public CsvColumnIterator(int index, Iterator<CSVRecord> it) {
            this.index = index;
            this.it = Objects.requireNonNull(it);
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public String next() {
            CSVRecord next = it.next();
            String stringValue = next.get(index);
            // treating empty string as null
            return stringValue.isEmpty() ? null : stringValue;
        }
    }
}

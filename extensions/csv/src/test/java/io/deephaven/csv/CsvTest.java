package io.deephaven.csv;

import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders3;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@RunWith(Parameterized.class)
public class CsvTest {

    private static final Instant TIMESTAMP_A = LocalDateTime.of(2021, 9, 27, 19, 0, 0).toInstant(ZoneOffset.UTC);
    private static final Instant TIMESTAMP_B = LocalDateTime.of(2021, 9, 27, 20, 0, 0).toInstant(ZoneOffset.UTC);

    @Parameters(name = "{0}")
    public static Iterable<Object[]> parameters() {
        return () -> tests().stream().map(CsvTest::parameterize).iterator();
    }

    public static List<CsvTest> tests() {
        return Arrays.asList(
                timestamp(),
                timestampSeconds(),
                timestampMillis(),
                timestampMicros(),
                timestampNanos(),
                timestampMixed(),
                timestampLegacy(),
                bools(),
                chars(),
                byteIsShort(),
                byteViaHeader(),
                byteViaInference(),
                shortRange(),
                intRange(),
                longRange(),
                longAsStringsViaInference(),
                longAsStringsViaParser(),
                longAsStringsViaHeader(),
                longOverrideStringInference(),
                doubleRange(),
                floatIsDouble(),
                floatViaHeader(),
                floatViaInference(),
                floatFromDouble(),
                strings(),
                stringsPound(),
                languageExample(),
                languageExampleTsv(),
                languageExampleHeaderless(),
                languageExampleHeaderlessExplicit(),
                whitespaceNoQuotes(),
                whitespaceNoQuotesLiteral(),
                whitespaceOutside(),
                whitespaceInsideDefault(),
                whitespaceInsideTrim(),
                whitespaceInsideAndOutsideDefault(),
                whitespaceInsideAndOutsideTrim());
    }

    public static CsvTest timestamp() {
        final NewTable expected = ColumnHeader.ofInstant("Timestamp")
                .row(TIMESTAMP_A)
                .row(null)
                .row(TIMESTAMP_B)
                .newTable();
        return new CsvTest("timestamp", "timestamp.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest timestampSeconds() {
        final NewTable expected = ColumnHeader.ofInstant("Timestamp")
                .row(TIMESTAMP_A)
                .row(null)
                .row(TIMESTAMP_B)
                .newTable();
        return new CsvTest("timestampSeconds", "timestamp-seconds.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest timestampMillis() {
        final NewTable expected = ColumnHeader.ofInstant("Timestamp")
                .row(TIMESTAMP_A)
                .row(null)
                .row(TIMESTAMP_B)
                .newTable();
        return new CsvTest("timestampMillis", "timestamp-millis.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest timestampMicros() {
        final NewTable expected = ColumnHeader.ofInstant("Timestamp")
                .row(TIMESTAMP_A)
                .row(null)
                .row(TIMESTAMP_B)
                .newTable();
        return new CsvTest("timestampMicros", "timestamp-micros.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest timestampNanos() {
        final NewTable expected = ColumnHeader.ofInstant("Timestamp")
                .row(TIMESTAMP_A)
                .row(null)
                .row(TIMESTAMP_B)
                .newTable();
        return new CsvTest("timestampNanos", "timestamp-nanos.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest timestampMixed() {
        // Can't infer milli and micros in a single column - will parse as a long, not an Instant.
        final NewTable expected = ColumnHeader.ofLong("Timestamp")
                .row(TIMESTAMP_A.toEpochMilli())
                .row(null)
                .row(TIMESTAMP_B.toEpochMilli() * 1000L)
                .newTable();
        return new CsvTest("timestampMixed", "timestamp-mixed.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest timestampLegacy() {
        final NewTable expected = ColumnHeader.ofInstant("Timestamp")
                .row(TIMESTAMP_A)
                .row(null)
                .row(TIMESTAMP_B)
                .newTable();
        return new CsvTest("timestampLegacy", "timestamp-legacy.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest bools() {
        final NewTable expected = ColumnHeader.ofBoolean("Bool")
                .row(true)
                .row(null)
                .row(false)
                .row(true)
                .row(false)
                .row(true)
                .row(false)
                .newTable();
        return new CsvTest("bools", "bools.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest chars() {
        final NewTable expected = ColumnHeader.ofChar("Char")
                .row('A')
                .row(null)
                .row('B')
                .row('C')
                .row('1')
                .row('2')
                .row('3')
                .newTable();
        return new CsvTest("chars", "chars.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest byteViaHeader() {
        final NewTable expected = ColumnHeader.ofByte("Byte")
                .row((byte) (Byte.MIN_VALUE + 1))
                .row(null)
                .row(Byte.MAX_VALUE)
                .newTable();
        return new CsvTest("byteViaHeader", "byte.csv", CsvSpecs.builder().header(expected.header()).build(), expected);
    }

    public static CsvTest byteViaInference() {
        final NewTable expected = ColumnHeader.ofByte("Byte")
                .row((byte) (Byte.MIN_VALUE + 1))
                .row(null)
                .row(Byte.MAX_VALUE)
                .newTable();
        return new CsvTest("byteViaInference", "byte.csv",
                CsvSpecs.builder().inference(InferenceSpecs.builder().addParsers(Parser.BYTE).build()).build(),
                expected);
    }

    public static CsvTest byteIsShort() {
        // By default, byte will be parsed as short
        final NewTable expected = ColumnHeader.ofShort("Byte")
                .row((short) (Byte.MIN_VALUE + 1))
                .row(null)
                .row((short) Byte.MAX_VALUE)
                .newTable();
        return new CsvTest("byteIsShort", "byte.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest shortRange() {
        final NewTable expected = ColumnHeader.ofShort("Short")
                .row((short) (Short.MIN_VALUE + 1))
                .row(null)
                .row(Short.MAX_VALUE)
                .newTable();
        return new CsvTest("shortRange", "short.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest intRange() {
        final NewTable expected = ColumnHeader.ofInt("Int")
                .row(Integer.MIN_VALUE + 1)
                .row(null)
                .row(Integer.MAX_VALUE)
                .newTable();
        return new CsvTest("intRange", "int.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest longRange() {
        final NewTable expected = ColumnHeader.ofLong("Long")
                .row(Long.MIN_VALUE + 1)
                .row(null)
                .row(Long.MAX_VALUE)
                .newTable();
        return new CsvTest("longRange", "long.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest longAsStringsViaInference() {
        final NewTable expected = ColumnHeader.ofString("Long")
                .row("-9223372036854775807")
                .row(null)
                .row("9223372036854775807")
                .newTable();
        return new CsvTest("longAsStringsViaInference", "long.csv",
                CsvSpecs.builder().inference(InferenceSpecs.strings()).build(), expected);
    }

    public static CsvTest longAsStringsViaParser() {
        final NewTable expected = ColumnHeader.ofString("Long")
                .row("-9223372036854775807")
                .row(null)
                .row("9223372036854775807")
                .newTable();
        return new CsvTest("longAsStringsViaParser", "long.csv",
                CsvSpecs.builder().putParsers("Long", Parser.STRING).build(), expected);
    }

    public static CsvTest longAsStringsViaHeader() {
        final NewTable expected = ColumnHeader.ofString("Long")
                .row("-9223372036854775807")
                .row(null)
                .row("9223372036854775807")
                .newTable();
        return new CsvTest("longAsStringsViaHeader", "long.csv", CsvSpecs.builder().header(expected.header()).build(),
                expected);
    }

    public static CsvTest longOverrideStringInference() {
        final NewTable expected = ColumnHeader.ofLong("Long")
                .row(Long.MIN_VALUE + 1)
                .row(null)
                .row(Long.MAX_VALUE)
                .newTable();
        final InferenceSpecs stringOnlyInference = InferenceSpecs.strings();
        final TableHeader tableHeader = ColumnHeader.ofLong("Long").tableHeader();
        final CsvSpecs csvSpecs = CsvSpecs.builder().inference(stringOnlyInference).header(tableHeader).build();
        return new CsvTest("longOverrideStringInference", "long.csv", csvSpecs, expected);
    }

    public static CsvTest floatIsDouble() {
        // By defaults, floats are parsed as double
        final NewTable expected = ColumnHeader.ofDouble("Float")
                .row((double) Float.POSITIVE_INFINITY)
                .row(null)
                .row((double) Float.NEGATIVE_INFINITY)
                .row((double) Float.NaN).row(3.4028235e+38d)
                .row(1.17549435E-38d)
                .row(1.4e-45d)
                .newTable();
        return new CsvTest("floatIsDouble", "floats.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest floatViaHeader() {
        final NewTable expected = ColumnHeader.ofFloat("Float")
                .row(Float.POSITIVE_INFINITY)
                .row(null)
                .row(Float.NEGATIVE_INFINITY)
                .row(Float.NaN)
                .row(Float.MAX_VALUE)
                .row(Float.MIN_NORMAL)
                .row(Float.MIN_VALUE)
                .newTable();
        return new CsvTest("floatViaHeader", "floats.csv", CsvSpecs.builder().header(expected.header()).build(),
                expected);
    }

    public static CsvTest floatViaInference() {
        final NewTable expected = ColumnHeader.ofFloat("Float")
                .row(Float.POSITIVE_INFINITY)
                .row(null)
                .row(Float.NEGATIVE_INFINITY)
                .row(Float.NaN)
                .row(Float.MAX_VALUE)
                .row(Float.MIN_NORMAL)
                .row(Float.MIN_VALUE)
                .newTable();
        return new CsvTest("floatViaInference", "floats.csv",
                CsvSpecs.builder().inference(InferenceSpecs.builder().addParsers(Parser.FLOAT).build()).build(),
                expected);
    }

    public static CsvTest floatFromDouble() {
        final NewTable expected = ColumnHeader.ofFloat("Double")
                .row((float) Double.POSITIVE_INFINITY)
                .row(null)
                .row((float) Double.NEGATIVE_INFINITY)
                .row((float) Double.NaN)
                .row((float) Double.MAX_VALUE)
                .row((float) Double.MIN_NORMAL)
                .row((float) Double.MIN_VALUE)
                .newTable();
        return new CsvTest("floatFromDouble", "doubles.csv", CsvSpecs.builder().header(expected.header()).build(),
                expected);
    }

    public static CsvTest doubleRange() {
        final NewTable expected = ColumnHeader.ofDouble("Double")
                .row(Double.POSITIVE_INFINITY)
                .row(null)
                .row(Double.NEGATIVE_INFINITY)
                .row(Double.NaN)
                .row(Double.MAX_VALUE)
                .row(Double.MIN_NORMAL)
                .row(Double.MIN_VALUE)
                .newTable();
        return new CsvTest("doubleRange", "doubles.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest strings() {
        final NewTable expected = ColumnHeader.ofString("String")
                .row("Hello, world")
                .row(null)
                .row("Goodbye.")
                .newTable();
        return new CsvTest("strings", "strings.csv", CsvSpecs.csv(), expected);
    }

    public static CsvTest stringsPound() {
        final NewTable expected = ColumnHeader.ofString("String")
                .row("Hello, world")
                .row(null)
                .row("Goodbye.")
                .newTable();
        return new CsvTest("stringsPound", "strings-pound.csv", CsvSpecs.builder().quote('#').build(), expected);
    }

    public static CsvTest languageExample() {
        return new CsvTest("languageExample", "language-example.csv", CsvSpecs.csv(), languageCreatorTypeTable());
    }

    public static CsvTest languageExampleTsv() {
        return new CsvTest("languageExampleTsv", "language-example.tsv", CsvSpecs.tsv(), languageCreatorTypeTable());
    }

    public static CsvTest languageExampleHeaderless() {
        return new CsvTest("languageExampleHeaderless", "language-example-headerless.csv", CsvSpecs.headerless(),
                languageCreatorTypeTableHeaderless());
    }

    public static CsvTest languageExampleHeaderlessExplicit() {
        final NewTable expected = languageCreatorTypeTable();
        final CsvSpecs specs = CsvSpecs.headerless(expected.header());
        return new CsvTest("languageExampleHeaderlessExplicit", "language-example-headerless.csv", specs, expected);
    }

    public static CsvTest whitespaceNoQuotes() {
        final NewTable expected = ColumnHeader.of(
                ColumnHeader.ofString("Sym"),
                ColumnHeader.ofString("Type"),
                ColumnHeader.ofDouble("Price"),
                ColumnHeader.ofShort("SecurityId"))
                .row("GOOG", "Dividend", 0.25, (short) 200)
                .row("T", "Dividend", 0.15, (short) 300)
                .row("Z", "Dividend", 0.18, (short) 500)
                .newTable();
        final CsvSpecs specs = CsvSpecs.csv();
        return new CsvTest("whitespaceNoQuotes", "whitespace-no-quotes.csv", specs, expected);
    }

    public static CsvTest whitespaceNoQuotesLiteral() {
        final NewTable expected = ColumnHeader.of(
                ColumnHeader.ofString("Sym"),
                ColumnHeader.ofString("Type"),
                ColumnHeader.ofString("Price"),
                ColumnHeader.ofString("SecurityId"))
                .row("GOOG", " Dividend", " 0.25", " 200")
                .row("T", " Dividend", " 0.15", " 300")
                .row(" Z", " Dividend", " 0.18", " 500")
                .newTable();
        final CsvSpecs specs = CsvSpecs.builder().ignoreSurroundingSpaces(false).build();
        return new CsvTest("whitespaceNoQuotesLiteral", "whitespace-no-quotes.csv", specs, expected);
    }

    public static CsvTest whitespaceOutside() {
        final NewTable expected = ColumnHeader.of(
                ColumnHeader.ofString("Sym"),
                ColumnHeader.ofString("Type"),
                ColumnHeader.ofDouble("Price"),
                ColumnHeader.ofShort("SecurityId"))
                .row("GOOG", "Dividend", 0.25, (short) 200)
                .row("T", "Dividend", 0.15, (short) 300)
                .row("Z", "Dividend", 0.18, (short) 500)
                .newTable();
        final CsvSpecs specs = CsvSpecs.csv();
        return new CsvTest("whitespaceOutside", "whitespace-outside.csv", specs, expected);
    }

    public static CsvTest whitespaceInsideDefault() {
        final NewTable expected = ColumnHeader.of(
                ColumnHeader.ofString("Sym"),
                ColumnHeader.ofString("Type"),
                ColumnHeader.ofString("Price"),
                ColumnHeader.ofString("SecurityId"))
                .row("GOOG", " Dividend", " 0.25", " 200")
                .row("T", " Dividend", " 0.15", " 300")
                .row(" Z", " Dividend", " 0.18", " 500")
                .newTable();
        final CsvSpecs specs = CsvSpecs.csv();
        return new CsvTest("whitespaceInsideDefault", "whitespace-inside.csv", specs, expected);
    }

    public static CsvTest whitespaceInsideTrim() {
        final NewTable expected = ColumnHeader.of(
                ColumnHeader.ofString("Sym"),
                ColumnHeader.ofString("Type"),
                ColumnHeader.ofDouble("Price"),
                ColumnHeader.ofShort("SecurityId"))
                .row("GOOG", "Dividend", 0.25, (short) 200)
                .row("T", "Dividend", 0.15, (short) 300)
                .row("Z", "Dividend", 0.18, (short) 500)
                .newTable();
        final CsvSpecs specs = CsvSpecs.builder().trim(true).build();
        return new CsvTest("whitespaceInsideTrim", "whitespace-inside.csv", specs, expected);
    }

    public static CsvTest whitespaceInsideAndOutsideDefault() {
        final NewTable expected = ColumnHeader.of(
                ColumnHeader.ofString("Sym"),
                ColumnHeader.ofString("Type"),
                ColumnHeader.ofString("Price"),
                ColumnHeader.ofString("SecurityId"))
                .row("GOOG", " Dividend", " 0.25", " 200")
                .row("T", " Dividend", " 0.15", " 300")
                .row(" Z", " Dividend", " 0.18", " 500")
                .newTable();
        final CsvSpecs specs = CsvSpecs.csv();
        return new CsvTest("whitespaceInsideAndOutsideDefault", "whitespace-inside-and-outside.csv", specs, expected);
    }

    public static CsvTest whitespaceInsideAndOutsideTrim() {
        final NewTable expected = ColumnHeader.of(
                ColumnHeader.ofString("Sym"),
                ColumnHeader.ofString("Type"),
                ColumnHeader.ofDouble("Price"),
                ColumnHeader.ofShort("SecurityId"))
                .row("GOOG", "Dividend", 0.25, (short) 200)
                .row("T", "Dividend", 0.15, (short) 300)
                .row("Z", "Dividend", 0.18, (short) 500)
                .newTable();
        final CsvSpecs specs = CsvSpecs.builder().trim(true).build();
        return new CsvTest("whitespaceInsideAndOutsideTrim", "whitespace-inside-and-outside.csv", specs, expected);
    }

    private static NewTable languageCreatorTypeTable() {
        return populateLanguageExample(ColumnHeader.ofString("Language")
                .header(ColumnHeader.ofString("Creator"))
                .header(ColumnHeader.ofString("Type")));
    }

    private static NewTable languageCreatorTypeTableHeaderless() {
        return populateLanguageExample(ColumnHeader.ofString("Column1")
                .header(ColumnHeader.ofString("Column2"))
                .header(ColumnHeader.ofString("Column3")));
    }

    private static NewTable populateLanguageExample(ColumnHeaders3<String, String, String> header) {
        return header
                .row("C", "Dennis Ritchie", "Compiled")
                .row("C++", "Bjarne Stroustrup", "Compiled")
                .row("Fortran", "John Backus", "Compiled")
                .row("Java", "James Gosling", "Both")
                .row("JavaScript", "Brendan Eich", "Interpreted")
                .row("MATLAB", "Cleve Moler", "Interpreted")
                .row("Pascal", "Niklas Wirth", "Compiled")
                .row("Python", "Guido van Rossum", "Interpreted")
                .newTable();
    }

    private final String name;
    private final String resourceName;
    private final CsvSpecs specs;
    private final NewTable expected;

    public CsvTest(String name, String resourceName, CsvSpecs specs, NewTable expected) {
        this.name = Objects.requireNonNull(name);
        this.resourceName = Objects.requireNonNull(resourceName);
        this.specs = Objects.requireNonNull(specs);
        this.expected = expected;
    }

    Object[] parameterize() {
        return new Object[] {name, resourceName, specs, expected};
    }

    @Test
    public void parseCsv() throws IOException {
        final InputStream in = CsvTest.class.getResourceAsStream(resourceName);
        if (in == null) {
            throw new IllegalArgumentException("Unable to find resource " + resourceName);
        }
        final NewTable actual;
        try (final Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
            actual = specs.parse(reader);
        } catch (Parser.ParserException e) {
            if (expected == null) {
                // expected!
                return;
            }
            throw e;
        }
        Assertions.assertThat(actual).isEqualTo(expected);
    }
}

package io.deephaven.csv;

import gnu.trove.list.array.*;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.parsers.IteratorHolder;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.Renderer;
import org.apache.commons.io.input.ReaderInputStream;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.*;

public class CsvReaderTest {
    private static class Sentinels {
        public static final byte NULL_BOOLEAN_AS_BYTE = Byte.MIN_VALUE;
        public static final byte NULL_BYTE = Byte.MIN_VALUE;
        public static final short NULL_SHORT = Short.MIN_VALUE;
        public static final int NULL_INT = Integer.MIN_VALUE;
        public static final long NULL_LONG = Long.MIN_VALUE;
        public static final float NULL_FLOAT = -Float.MAX_VALUE;
        public static final double NULL_DOUBLE = -Double.MAX_VALUE;
        public static final char NULL_CHAR = Character.MAX_VALUE;
        public static final long NULL_DATETIME_AS_LONG = Long.MIN_VALUE;
        public static final long NULL_TIMESTAMP_AS_LONG = Long.MIN_VALUE;
    }

    private static final String BOOLEAN_INPUT = "" +
            "Values\n" +
            "true\n" +
            "\n" +
            "false\n" +
            "True\n" +
            "False\n" +
            "TrUe\n" +
            "FALSE\n";

    @Test
    public void booleans() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (byte) 1, Sentinels.NULL_BOOLEAN_AS_BYTE, (byte) 0, (byte) 1, (byte) 0,
                        (byte) 1, (byte) 0).reinterpret(boolean.class));

        invokeTest(defaultCsvReader(), BOOLEAN_INPUT, expected);
    }

    private static final String CHAR_INPUT = "" +
            "Values\n" +
            "A\n" +
            "\n" +
            "B\n" +
            "C\n" +
            "1\n" +
            "2\n" +
            "3\n";

    @Test
    public void chars() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", 'A', Sentinels.NULL_CHAR, 'B', 'C', '1', '2', '3'));

        invokeTest(defaultCsvReader(), CHAR_INPUT, expected);
    }

    @Test
    public void forbiddenNullChars() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "A\n" +
                Sentinels.NULL_CHAR + "\n";

        // NULL_CHAR can't be parsed as char; will be promoted to String.
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values", "A", "" + Sentinels.NULL_CHAR));

        invokeTest(defaultCsvReader(), input, expected);
    }

    private static final String BYTE_INPUT = "" +
            "Values\n" +
            "-127\n" +
            "\n" +
            "127\n";

    @Test
    public void byteViaInference() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (byte) (Byte.MIN_VALUE + 1), Sentinels.NULL_BYTE, Byte.MAX_VALUE));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE), BYTE_INPUT, expected);
    }

    @Test
    public void forbiddenNullBytes() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "-127\n" +
                Sentinels.NULL_BYTE + "\n" +
                "127\n";
        // NULL_BYTE can't be parsed as char; will be promoted to short (because we're using
        // the Parsers.COMPLETE set of parsers, and short is in Parsers.COMPLETE set).
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (short) (Byte.MIN_VALUE + 1), Sentinels.NULL_BYTE, Byte.MAX_VALUE));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE), input, expected);
    }

    @Test
    public void byteIsInt() throws CsvReaderException {
        // By default, byte will be parsed as int, because neither Parsers.BYTE nor Parsers.SHORT is in Parsers.DEFAULT
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (Byte.MIN_VALUE + 1), Sentinels.NULL_INT, Byte.MAX_VALUE));

        invokeTest(defaultCsvReader(), BYTE_INPUT, expected);
    }

    private static final String SHORT_INPUT = "" +
            "Values\n" +
            "-32767\n" +
            "\n" +
            "32767\n";

    @Test
    public void shorts() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (short) (Short.MIN_VALUE + 1), Sentinels.NULL_SHORT, Short.MAX_VALUE));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE), SHORT_INPUT, expected);
    }

    @Test
    public void forbiddenNullShorts() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "-32767\n" +
                Sentinels.NULL_SHORT + "\n" +
                "32767\n";

        // NULL_SHORT can't be parsed as short; will be promoted to int.
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (int) (Short.MIN_VALUE + 1), Sentinels.NULL_SHORT, Short.MAX_VALUE));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE), input, expected);
    }

    @Test
    public void ints() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "-2147483647\n" +
                "\n" +
                "2147483647\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", Integer.MIN_VALUE + 1, Sentinels.NULL_INT, Integer.MAX_VALUE));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void forbiddenNullInts() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                Sentinels.NULL_INT + "\n";

        // NULL_INT can't be parsed as int; will be promoted to long.
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (long) Sentinels.NULL_INT));

        invokeTest(defaultCsvReader(), input, expected);
    }

    private static final String LONG_INPUT = "" +
            "Values\n" +
            "-9223372036854775807\n" +
            "\n" +
            "9223372036854775807\n";

    @Test
    public void longs() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", Long.MIN_VALUE + 1, Sentinels.NULL_LONG, Long.MAX_VALUE));

        invokeTest(defaultCsvReader(), LONG_INPUT, expected);
    }

    @Test
    public void forbiddenNullLongs() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                Sentinels.NULL_LONG + "\n";

        // NULL_LONG can't be parsed as long; will be promoted to double.
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", (double) Sentinels.NULL_LONG));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void longAsStringsViaInference() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values", "-9223372036854775807", null, "9223372036854775807"));

        invokeTest(defaultCsvReader().setParsers(List.of(Parsers.STRING)), LONG_INPUT, expected);
    }

    @Test
    public void longAsStringsViaParser() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values", "-9223372036854775807", null, "9223372036854775807"));

        invokeTest(defaultCsvReader().setParserFor("Values", Parsers.STRING), LONG_INPUT, expected);
    }

    private static final String FLOAT_INPUT = "" +
            "Values\n" +
            "Infinity\n" +
            "\n" +
            "-Infinity\n" +
            "NaN\n" +
            "3.4028234e+38\n" +
            "1.17549435E-38\n" +
            "1.4e-45\n";

    @Test
    public void floatIsDouble() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values",
                        Float.POSITIVE_INFINITY,
                        Sentinels.NULL_DOUBLE,
                        Float.NEGATIVE_INFINITY,
                        Float.NaN,
                        3.4028234e+38d,
                        1.17549435E-38d,
                        1.4e-45d));

        invokeTest(defaultCsvReader(), FLOAT_INPUT, expected);
    }

    @Test
    public void floatViaInference() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values",
                        Float.POSITIVE_INFINITY,
                        Sentinels.NULL_FLOAT,
                        Float.NEGATIVE_INFINITY,
                        Float.NaN,
                        Float.MAX_VALUE,
                        Float.MIN_NORMAL,
                        Float.MIN_VALUE));

        invokeTest(defaultCsvReader().setParsers(List.of(Parsers.FLOAT_FAST)), FLOAT_INPUT, expected);
    }

    @Test
    public void forbiddenNullFloats() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                Sentinels.NULL_FLOAT + "\n";

        // I wanted to say simply (double)Sentinels.NULL_FLOAT, but that's a different number from
        // the below (alas).
        final double nullFloatAsParsedByDouble = Double.parseDouble("" + Sentinels.NULL_FLOAT);

        // NULL_FLOAT can't be parsed as float; will be promoted to double.
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", nullFloatAsParsedByDouble));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE), input, expected);
    }

    @Test
    public void doubleRange() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "Infinity\n" +
                "\n" +
                "-Infinity\n" +
                "NaN\n" +
                "1.7976931348623157e+308\n" +
                "2.2250738585072014E-308\n" +
                "4.9e-324\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values",
                        Double.POSITIVE_INFINITY,
                        Sentinels.NULL_DOUBLE,
                        Double.NEGATIVE_INFINITY,
                        Double.NaN,
                        Double.MAX_VALUE,
                        Double.MIN_NORMAL,
                        Double.MIN_VALUE));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void forbiddenNullDoubles() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                Sentinels.NULL_DOUBLE + "\n";

        // NULL_DOUBLE can't be parsed as double; will be promoted to String
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values", Sentinels.NULL_DOUBLE + ""));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE), input, expected);
    }

    @Test
    public void varietyOfNumerics() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "\n" + // NULL
                "\n" + // NULL
                "0\n" + // byte
                "1\n" + // byte
                "300\n" + // short
                "400\n"; // short
        // "100000\n" + // int
        // "100001\n" + // int
        // "3000000000\n" + // long
        // "123.456\n" + // float
        // "1234.5678\n"; // double

        // NULL_DOUBLE can't be parsed as double; will be promoted to String
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", Sentinels.NULL_SHORT, Sentinels.NULL_SHORT, (short) 0, (short) 1, (short) 300,
                        (short) 400));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE), input, expected);
    }


    @Test
    public void strings() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "\"Hello, world\"\n" +
                "\n" + // the empty string is null
                "Goodbye.\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values",
                        "Hello, world",
                        null,
                        "Goodbye."));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void multi() throws CsvReaderException {
        // These are columns of data. We are going to mix and match them.
        final String booleanInput = "false\ntrUe\nFaLsE\n";
        final String byteInput1 = "1\n2\n3\n";
        final String byteInput2 = "-1\n-2\n-3\n";
        final String shortInput = "300\n301\n302\n";
        final String intInput = "50000\n50001\n50002\n";
        final String longInput = "3000000000\n3000000001\n3000000002\n";
        final String doubleInput = "123.456\n234.567e25\n987.654e-20\n";
        final String dateTimeInput = "1966-03-01 12:34:56Z\n1977-02-08 03:04:05Z\n1989-11-11 11:11:11Z\n";
        final String charInput = "a\nb\nc\n";
        final String stringInput = "Deephaven\nStreaming\nJoins\n";

        final String[] allInputs = {
                booleanInput, byteInput1, byteInput2, shortInput, intInput, longInput, doubleInput, dateTimeInput,
                charInput, stringInput
        };
        final Class<?>[] expectedTypes = {
                boolean.class, byte.class, byte.class, short.class, int.class, long.class, double.class,
                Instant.class, char.class, String.class
        };
        final boolean[] entriesAreAllNullOrOneChar = {
                false, true, false, false, false, false, false, false, true, false
        };

        for (int ii = 0; ii < allInputs.length; ++ii) {
            for (int jj = 0; jj < allInputs.length; ++jj) {
                final boolean oneCharIJ = entriesAreAllNullOrOneChar[ii] && entriesAreAllNullOrOneChar[jj];
                final Class<?> inferredIJ = SimpleInferrer.infer(expectedTypes[ii], expectedTypes[jj], oneCharIJ);
                for (int kk = 0; kk < allInputs.length; ++kk) {
                    final boolean oneCharIJK = oneCharIJ && entriesAreAllNullOrOneChar[kk];
                    final Class<?> expectedType = SimpleInferrer.infer(expectedTypes[kk], inferredIJ, oneCharIJK);
                    final String input = "Values\n" + allInputs[ii] + allInputs[jj] + allInputs[kk];
                    final CsvReader csvReader = defaultCsvReader().setParsers(Parsers.COMPLETE);
                    final ColumnSet columnSet = parse(csvReader, input);
                    final Class<?> actualType = columnSet.columns[0].reinterpretedType;
                    Assertions.assertThat(actualType)
                            .withFailMessage("Expected to infer type %s; actually inferred %s. Failing input: %s",
                                    expectedType.getCanonicalName(), actualType.getCanonicalName(), input)
                            .isEqualTo(expectedType);
                }
            }
        }
    }

    private static class SimpleInferrer {
        private static final int BOOLEAN = 1;
        private static final int DATETIME = 2;
        private static final int STRING = 3;
        private static final int CHAR = 4;
        private static final int BYTE = 5;
        private static final int SHORT = 6;
        private static final int INT = 7;
        private static final int LONG = 8;
        private static final int FLOAT = 9;
        private static final int DOUBLE = 10;

        public static Class<?> infer(final Class<?> type1, final Class<?> type2, final boolean allNullOrOneChar) {
            // Same types yield that type.
            if (type1 == type2) {
                return type1;
            }

            final int priority1 = getPriority(type1);
            final int priority2 = getPriority(type2);

            final int highestPriority = Math.min(priority1, priority2);
            final Class<?> widestType = priority1 < priority2 ? type2 : type1;

            // (Boolean, DateTime, or String) and (something else) yields String.
            if (highestPriority == BOOLEAN || highestPriority == DATETIME || highestPriority == STRING) {
                return String.class;
            }

            // Char paired with some numeric will yield char if the numerics are one digit wide; otherwise String
            if (highestPriority == CHAR) {
                return allNullOrOneChar ? char.class : String.class;
            }

            // Numeric types yield the widest type.
            return widestType;
        }

        private static int getPriority(Class<?> type) {
            if (type == boolean.class)
                return BOOLEAN;
            if (type == Instant.class)
                return DATETIME;
            if (type == char.class)
                return CHAR;
            if (type == String.class)
                return STRING;
            if (type == byte.class)
                return BYTE;
            if (type == short.class)
                return SHORT;
            if (type == int.class)
                return INT;
            if (type == long.class)
                return LONG;
            if (type == float.class)
                return FLOAT;
            if (type == double.class)
                return DOUBLE;
            throw new RuntimeException("Unexpected type " + type.getCanonicalName());
        }
    }

    @Test
    public void quotingSuccessfulEdgeCases() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "##\n" + // the empty string, which is configured below to give us NULL
                "####\n" + // #
                "######\n"; // ##

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values",
                        null,
                        "#",
                        "##"));

        invokeTest(defaultCsvReader().setquoteChar('#'), input, expected);
    }

    @Test
    public void quotingFailingEdgeCases() {
        final String input = "" +
                "Values\n" +
                "###\n"; // invalid

        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader().setquoteChar('#'), input, ColumnSet.NONE))
                .hasRootCauseMessage("Cell did not have closing quote character");
    }

    @Test
    public void quotingExcessMaterial() {
        final String input = "" +
                "Val1,Val2\n" +
                "#hello#junk,there\n"; // invalid

        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader().setquoteChar('#'), input, ColumnSet.NONE))
                .hasRootCauseMessage("Logic error: final non-whitespace in field is not quoteChar");
    }

    @Test
    public void stringWithNullLiteralSetAndValueNull() throws CsvReaderException {
        // It should work when the null literal is set to something special, but the null String value is the null
        // reference.
        final String input = "" +
                "Values\n" +
                "hello\n" +
                "NULL\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values", "hello", null));

        invokeTest(new CsvReader().setNullValueLiteral("NULL"), input, expected);
    }

    @Test
    public void stringsPound() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "#Hello, world#\n" +
                "\n" +
                "Goodbye.\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Values",
                        "Hello, world",
                        null,
                        "Goodbye."));

        invokeTest(defaultCsvReader().setquoteChar('#'), input, expected);
    }


    @Test
    public void newlineDiversity() throws CsvReaderException {
        final String input = "" +
                "Values\r" +
                "-2147483647\r\n" +
                "\n" +
                "2147483647\r\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", Integer.MIN_VALUE + 1, Sentinels.NULL_INT, Integer.MAX_VALUE));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void overrideHeaders() throws CsvReaderException {
        final String input = "" +
                "Foo,Bar,Baz\n" +
                "1,2,3\n" +
                "4,5,6\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("A", 1, 4),
                Column.ofValues("Qux", 2, 5),
                Column.ofValues("C", 3, 6));

        invokeTest(defaultCsvReader()
                .setHeaders("A", "B", "C")
                .setHeader(2, "Qux"), input, expected);
    }


    private static final String LANGUAGE_EXAMPLE_HEADERLESS_INPUT = "" +
            "C,Dennis Ritchie,Compiled\n" +
            "C++,Bjarne Stroustrup,Compiled\n" +
            "Fortran,John Backus,Compiled\n" +
            "Java,James Gosling,Both\n" +
            "JavaScript,Brendan Eich,Interpreted\n" +
            "MATLAB,Cleve Moler,Interpreted\n" +
            "Pascal,Niklaus Wirth,Compiled\n" +
            "Python,Guido van Rossum,Interpreted\n";

    private static final String LANGUAGE_EXAMPLE_INPUT = "" +
            "Language,Creator,Type\n" +
            LANGUAGE_EXAMPLE_HEADERLESS_INPUT;

    private static final String LANGUAGE_EXAMPLE_TSV = "" +
            "Language\tCreator\tType\n" +
            "C\tDennis Ritchie\tCompiled\n" +
            "C++\tBjarne Stroustrup\tCompiled\n" +
            "Fortran\tJohn Backus\tCompiled\n" +
            "Java\tJames Gosling\tBoth\n" +
            "JavaScript\tBrendan Eich\tInterpreted\n" +
            "MATLAB\tCleve Moler\tInterpreted\n" +
            "Pascal\tNiklaus Wirth\tCompiled\n" +
            "Python\tGuido van Rossum\tInterpreted\n";

    @Test
    public void languageExample() throws CsvReaderException {
        invokeTest(defaultCsvReader(), LANGUAGE_EXAMPLE_INPUT, languageCreatorTypeTable());
    }

    @Test
    public void languageExampleTsv() throws CsvReaderException {
        invokeTest(defaultCsvReader().setFieldDelimiter('\t'), LANGUAGE_EXAMPLE_TSV, languageCreatorTypeTable());
    }

    @Test
    public void languageExampleHeaderless() throws CsvReaderException {
        invokeTest(defaultCsvReader().setHasHeaders(false), LANGUAGE_EXAMPLE_HEADERLESS_INPUT,
                languageCreatorTypeTableHeaderless());
    }

    @Test
    public void languageExampleHeaderlessExplicit() throws CsvReaderException {
        final ColumnSet expected = languageCreatorTypeTable();
        invokeTest(defaultCsvReader()
                .setHasHeaders(false)
                .setHeaders(List.of("Language", "Creator", "Type")),
                LANGUAGE_EXAMPLE_HEADERLESS_INPUT, expected);
    }

    private static ColumnSet languageCreatorTypeTable() {
        return populateLanguageExample("Language", "Creator", "Type");
    }

    private static ColumnSet languageCreatorTypeTableHeaderless() {
        return populateLanguageExample("Column1", "Column2", "Column3");
    }

    private static ColumnSet populateLanguageExample(final String col1, final String col2, final String col3) {
        return ColumnSet.of(
                Column.ofRefs(col1, "C", "C++", "Fortran", "Java",
                        "JavaScript", "MATLAB", "Pascal", "Python"),
                Column.ofRefs(col2, "Dennis Ritchie", "Bjarne Stroustrup", "John Backus", "James Gosling",
                        "Brendan Eich", "Cleve Moler", "Niklaus Wirth", "Guido van Rossum"),
                Column.ofRefs(col3, "Compiled", "Compiled", "Compiled", "Both",
                        "Interpreted", "Interpreted", "Compiled", "Interpreted"));
    }

    private static final String WHITESPACE_NO_QUOTES = "" +
            "Sym,Type,Price,SecurityId\n" +
            "GOOG, Dividend, 0.25, 200\n" +
            "T, Dividend, 0.15, 300\n" +
            " Z, Dividend, 0.18, 500\n";

    @Test
    public void whitespaceNoQuotes() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Sym", "GOOG", "T", "Z"),
                Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                Column.ofValues("Price", 0.25, 0.15, 0.18),
                Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvReader(), WHITESPACE_NO_QUOTES, expected);
    }

    @Test
    public void whitespaceNoQuotesLiteral() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Sym", "GOOG", "T", " Z"),
                Column.ofRefs("Type", " Dividend", " Dividend", " Dividend"),
                Column.ofValues("Price", 0.25, 0.15, 0.18),
                Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvReader().setIgnoreSurroundingSpaces(false), WHITESPACE_NO_QUOTES, expected);
    }

    @Test
    public void whitespaceOutside() throws CsvReaderException {
        // Use vertical bars instead of quotation marks to make things more readable for the humans looking at this.
        final String input = ("" +
                "Sym,Type,Price,SecurityId\n" +
                "|GOOG|, |Dividend|, |0.25|, |200|\n" +
                "|T|, |Dividend|, |0.15|, |300|\n" +
                " |Z|, |Dividend|, |0.18|, |500|\n");

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Sym", "GOOG", "T", "Z"),
                Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                Column.ofValues("Price", 0.25, 0.15, 0.18),
                Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvReader().setquoteChar('|'), input, expected);
    }

    // Use vertical bars instead of quotation marks to make things more readable for the humans looking at this.
    private static final String WHITESPACE_INSIDE = "" +
            "Sym,Type,Price,SecurityId\n" +
            "|GOOG|,| Dividend|,| 0.25|,| 200|\n" +
            "|T|,|Dividend |,| 0.15|,| 300|\n" +
            "| Z|,| Dividend |,| 0.18|,| 500|\n";

    @Test
    public void whitespaceInsideDefault() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Sym", "GOOG", "T", " Z"),
                Column.ofRefs("Type", " Dividend", "Dividend ", " Dividend "),
                Column.ofValues("Price", 0.25, 0.15, 0.18),
                Column.ofValues("SecurityId", 200, 300, 500));
        invokeTest(defaultCsvReader().setquoteChar('|'), WHITESPACE_INSIDE, expected);
    }

    @Test
    public void whitespaceInsideTrim() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Sym", "GOOG", "T", "Z"),
                Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                Column.ofValues("Price", 0.25, 0.15, 0.18),
                Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvReader().setquoteChar('|').setTrim(true), WHITESPACE_INSIDE, expected);
    }

    private static final String WHITESPACE_INSIDE_AND_OUTSIDE = "" +
            "Sym,Type,Price,SecurityId\n" +
            "|GOOG|, | Dividend|, | 0.25|, | 200|\n" +
            "|T|, | Dividend|, | 0.15|, | 300|\n" +
            "| Z|, | Dividend|, | 0.18|, | 500|\n";

    @Test
    public void whitespaceInsideAndOutsideDefault() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Sym", "GOOG", "T", " Z"),
                Column.ofRefs("Type", " Dividend", " Dividend", " Dividend"),
                Column.ofValues("Price", 0.25, 0.15, 0.18),
                Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvReader().setquoteChar('|'), WHITESPACE_INSIDE_AND_OUTSIDE, expected);
    }

    @Test
    public void whitespaceInsideAndOutsideTrim() throws CsvReaderException {
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Sym", "GOOG", "T", "Z"),
                Column.ofRefs("Type", "Dividend", "Dividend", "Dividend"),
                Column.ofValues("Price", 0.25, 0.15, 0.18),
                Column.ofValues("SecurityId", 200, 300, 500));

        invokeTest(defaultCsvReader().setquoteChar('|').setTrim(true), WHITESPACE_INSIDE_AND_OUTSIDE, expected);
    }

    @Test
    public void noTrailingNewlineHeaderOnly() throws CsvReaderException {
        // Sometimes there is no trailing newline. That's OK.
        final String input = "" +
                "Values1,Values2";

        final ColumnSet expected = ColumnSet.of(
                Column.ofArray("Values1", new short[0]),
                Column.ofArray("Values2", new short[0]));

        invokeTest(defaultCsvReader().setNullParser(Parsers.SHORT), input, expected);
    }

    @Test
    public void noTrailingNewline() throws CsvReaderException {
        // Sometimes there is no trailing newline. That's OK.
        final String input = "" +
                "SomeInts,SomeStrings\n" +
                "-3,foo\n" +
                "4,bar\n" +
                "-5,baz";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("SomeInts", -3, 4, -5),
                Column.ofRefs("SomeStrings", "foo", "bar", "baz"));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void tooFewColumnsWithFinalNewline() throws CsvReaderException {
        // If there are too few columns, we just pad with nulls.
        final String input = "" +
                "A,B,C,D\n" +
                "-3,foo,1.2,false\n" +
                "4,bar,3.4,true\n" +
                "-5\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("A", -3, 4, -5),
                Column.ofRefs("B", "foo", "bar", null),
                Column.ofValues("C", 1.2, 3.4, Sentinels.NULL_DOUBLE),
                Column.ofValues("D", (byte) 0, (byte) 1, Sentinels.NULL_BOOLEAN_AS_BYTE).reinterpret(boolean.class));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void tooFewColumnsWithoutFinalNewline() throws CsvReaderException {
        // If there are too few columns, we just pad with nulls.
        final String input = "" +
                "A,B,C,D\n" +
                "-3,foo,1.2,false\n" +
                "4,bar,3.4,true\n" +
                "-5";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("A", -3, 4, -5),
                Column.ofRefs("B", "foo", "bar", null),
                Column.ofValues("C", 1.2, 3.4, Sentinels.NULL_DOUBLE),
                Column.ofValues("D", (byte) 0, (byte) 1, Sentinels.NULL_BOOLEAN_AS_BYTE).reinterpret(boolean.class));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void tooManyColumns() {
        // Too many columns is an error.
        final String input = "" +
                "SomeInts,SomeStrings\n" +
                "-3,foo\n" +
                "4,bar,quz\n" +
                "-5,baz\n";

        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader(), input, ColumnSet.NONE))
                .hasRootCauseMessage("Row 3 has too many columns (expected 2)");
    }

    @Test
    public void duplicateColumnName() {
        final String input = "" +
                "abc,xyz,abc\n" +
                "Hello,there,Deephaven\n";
        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader(), input, ColumnSet.NONE))
                .hasMessageContaining("Repeated headers: abc");
    }

    @Test
    public void trailingNullColumnElided() throws CsvReaderException {
        // A completely-empty rightmost column (corresponding to a text file with trailing field delimiters on every
        // line) will just be dropped.
        final String input = "" +
                "abc,def,ghi,\n" +
                "Hello,there,Deephaven,\n" +
                "foo,bar,baz,\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("abc", "Hello", "foo"),
                Column.ofRefs("def", "there", "bar"),
                Column.ofRefs("ghi", "Deephaven", "baz"));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void trailingNullColumnMustBeEmpty() {
        // A completely-empty rightmost column (corresponding to a text file with trailing field delimiters on every
        // line) will just be dropped.
        final String input = "" +
                "abc,def,ghi,\n" +
                "Hello,there,Deephaven,\n" +
                "foo,bar,baz,nonempty\n";
        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader(), input, ColumnSet.NONE))
                .hasRootCauseMessage("Column assumed empty but contains data");
    }

    @Test
    public void dateTimes() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "2021-09-27T19:00:00Z\n" +
                "\n" +
                "2021-09-27T20:00:00Z\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", 1632769200000000000L, Sentinels.NULL_LONG, 1632772800000000000L)
                        .reinterpret(Instant.class));

        invokeTest(defaultCsvReader(), input, expected);
    }

    @Test
    public void dateTimeFormats() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "20210927T19Z\n" +
                "20210927 19Z\n" +
                "20210927T1934Z\n" +
                "20210927T193458Z\n" +
                "20210927T193458.123Z\n" +
                "20210927T193458.123456Z\n" +
                "20210927T193458.123456789Z\n" +
                "20210927T193458.123456789+0200\n" +
                "20210927T193458.123456789-0330\n" +

                "2021-09-27T19Z\n" +
                "2021-09-27 19Z\n" +
                "2021-09-27T19:34Z\n" +
                "2021-09-27T19:34:58Z\n" +
                "2021-09-27T19:34:58.123Z\n" +
                "2021-09-27T19:34:58.123456Z\n" +
                "2021-09-27T19:34:58.123456789Z\n" +
                "2021-09-27T19:34:58.123456789+0200\n" +
                "2021-09-27T19:34:58.123456789-0330\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values",
                        1632769200000000000L,
                        1632769200000000000L,
                        1632771240000000000L,
                        1632771298000000000L,
                        1632771298123000000L,
                        1632771298123456000L,
                        1632771298123456789L,
                        1632764098123456789L,
                        1632783898123456789L,

                        1632769200000000000L,
                        1632769200000000000L,
                        1632771240000000000L,
                        1632771298000000000L,
                        1632771298123000000L,
                        1632771298123456000L,
                        1632771298123456789L,
                        1632764098123456789L,
                        1632783898123456789L)
                        .reinterpret(Instant.class));

        invokeTest(defaultCsvReader(), input, expected);
    }


    @Test
    public void timestampSeconds() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "1632769200\n" +
                "\n" +
                "1632772800\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", 1632769200000000000L, Sentinels.NULL_TIMESTAMP_AS_LONG,
                        1632772800000000000L).reinterpret(Instant.class));

        invokeTest(defaultCsvReader().setParsers(List.of(Parsers.TIMESTAMP_SECONDS)), input, expected);
    }

    @Test
    public void timestampMillis() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "1632769200000\n" +
                "\n" +
                "1632772800000\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", 1632769200000000000L, Sentinels.NULL_TIMESTAMP_AS_LONG,
                        1632772800000000000L).reinterpret(Instant.class));

        invokeTest(defaultCsvReader().setParsers(List.of(Parsers.TIMESTAMP_MILLIS)), input, expected);
    }

    @Test
    public void timestampMicros() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "1632769200000000\n" +
                "\n" +
                "1632772800000000\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", 1632769200000000000L, Sentinels.NULL_TIMESTAMP_AS_LONG,
                        1632772800000000000L).reinterpret(Instant.class));

        invokeTest(defaultCsvReader().setParsers(List.of(Parsers.TIMESTAMP_MICROS)), input, expected);
    }

    @Test
    public void timestampNanos() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "1632769200000000000\n" +
                "\n" +
                "1632772800000000000\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", 1632769200000000000L, Sentinels.NULL_TIMESTAMP_AS_LONG,
                        1632772800000000000L).reinterpret(Instant.class));

        invokeTest(defaultCsvReader().setParsers(List.of(Parsers.TIMESTAMP_NANOS)), input, expected);
    }

    @Test
    public void dateTimeCustomizedTimezone() throws CsvReaderException {
        final String input = "" +
                "Values\n" +
                "2021-09-27T19:00:00 UTC\n" +
                "\n" +
                "2021-09-27T20:00:00 UTC\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", 1632769200000000000L, Sentinels.NULL_LONG, 1632772800000000000L)
                        .reinterpret(Instant.class).reinterpret(Instant.class));

        // Simple custom time zone parser that only understands " UTC"
        Tokenizer.CustomTimeZoneParser myTimeZoneParser = (bs, tzo, off) -> {
            if (bs.size() < 4) {
                return false;
            }
            final byte[] d = bs.data();
            final int o = bs.begin();
            if (d[o] == ' ' && d[o + 1] == 'U' && d[o + 2] == 'T' && d[o + 3] == 'C') {
                tzo.setValue(ZoneOffset.UTC);
                off.setValue(0);
                bs.setBegin(bs.begin() + 4);
                return true;
            }
            return false;
        };

        invokeTest(defaultCsvReader()
                .setCustomTimeZoneParser(myTimeZoneParser),
                input, expected);
    }

    private static final String ALL_NULLS = "" +
            "Values\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n" +
            "\n";

    @Test
    public void unparseable() {
        final String input = "" +
                "Values\n" +
                "hello\n" +
                "there\n";

        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader()
                .setParsers(List.of(Parsers.INT, Parsers.LONG, Parsers.DATETIME)), input, ColumnSet.NONE));
    }

    @Test
    public void noParsers() {
        final String input = "" +
                "Values\n" +
                "hello\n" +
                "there\n";

        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader().setParsers(List.of()), input, ColumnSet.NONE))
                .hasRootCauseMessage("No available parsers.");
    }

    @Test
    public void allNullsWithSpecifiedParser() throws CsvReaderException {
        final long nv = Sentinels.NULL_LONG;
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", nv, nv, nv, nv, nv));

        invokeTest(defaultCsvReader().setParserFor("Values", Parsers.LONG), ALL_NULLS, expected);
    }

    @Test
    public void allNullsWithNullParser() throws CsvReaderException {
        final long nv = Sentinels.NULL_LONG;
        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Values", nv, nv, nv, nv, nv));

        invokeTest(defaultCsvReader().setNullParser(Parsers.LONG), ALL_NULLS, expected);
    }

    @Test
    public void allNullsButNoParser() {
        Assertions.assertThatThrownBy(() -> invokeTest(defaultCsvReader(), ALL_NULLS, ColumnSet.NONE))
                .hasRootCauseMessage(
                        "Column contains all null cells: can't infer type of column, and nullParser is not set.");
    }

    @Test
    public void emptyTableWithSpecifiedParser() throws CsvReaderException {
        final String input = "Values\n";
        final ColumnSet expected = ColumnSet.of(
                Column.ofArray("Values", new long[0]));

        invokeTest(defaultCsvReader().setParserFor("Values", Parsers.LONG), input, expected);
    }

    @Test
    public void unicode() throws CsvReaderException {
        final String input = "" +
                "Emojis\n" +
                "Hello 💖\n" +
                "Regular ASCII\n" +
                "✨ Deephaven ✨\n" +
                "🎆🎆🎆🎆🎆\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("Emojis", "Hello 💖", "Regular ASCII", "✨ Deephaven ✨", "🎆🎆🎆🎆🎆"));

        invokeTest(defaultCsvReader(), input, expected);
    }

    /**
     * Test that input will be parsed as a char so long as it is in the BMP. The input is "tricky" because it starts out
     * looking like integrals.
     */
    @Test
    public void unicodeChars() throws CsvReaderException {
        // So long as a character is in the BMP (i.e. <= U+FFFF), it will be parsed as a char column.
        final String input = "" +
                "BMPChar\n" +
                "1\n" +
                "2\n" +
                "3\n" +
                "X\n" +
                "✈\n" +
                "❎\n" +
                "➉\n" +
                "✈\n" +
                "✨\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("BMPChar", '1', '2', '3', 'X', '✈', '❎', '➉', '✈', '✨'));

        invokeTest(defaultCsvReader(), input, expected);
    }

    /**
     * Large cells (10K characters or so), some with fancy Unicode, quotes, and escaped quotes.
     */
    @Test
    public void largeCells() throws CsvReaderException {
        final StringBuilder sbBytes = new StringBuilder();
        final StringBuilder sbChars = new StringBuilder();
        final StringBuilder sbQuotesEscaped = new StringBuilder();
        final StringBuilder sbQuotesLiteral = new StringBuilder();
        for (int ii = 0; ii < 1000; ++ii) {
            sbBytes.append("Deephaven!");
            sbChars.append("🍣Deep🍔haven!🍕");
            sbQuotesEscaped.append("Deep\"\"haven!");
            sbQuotesLiteral.append("Deep\"haven!");
        }
        final String largeCellBytes = sbBytes.toString();
        final String largeCellChars = sbChars.toString();
        final String largeCellEscaped = '"' + sbQuotesEscaped.toString() + '"';
        final String largeCellLiteral = sbQuotesLiteral.toString();

        final String input = "" +
                "LargeEmojis\n" +
                largeCellBytes + "\n" +
                largeCellChars + "\n" +
                largeCellEscaped + "\n" +
                largeCellBytes + "\n" +
                largeCellChars + "\n" +
                largeCellEscaped + "\n";

        System.out.println(input);
        final ColumnSet expected = ColumnSet.of(
                Column.ofRefs("LargeEmojis", largeCellBytes, largeCellChars, largeCellLiteral,
                        largeCellBytes, largeCellChars, largeCellLiteral));

        invokeTest(defaultCsvReader(), input, expected);
    }

    /**
     * Test the global null literal value.
     */
    @Test
    public void customGlobalNullValue() throws CsvReaderException {
        final String input = "" +
                "SomeBytes,SomeShorts,SomeInts,SomeLongs\n" +
                "1,2,3,4\n" +
                "NULL,NULL,NULL,NULL\n" +
                "100,32000,2000000000,4000000000\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("SomeBytes", (byte) 1, Sentinels.NULL_BYTE, (byte) 100),
                Column.ofValues("SomeShorts", (short) 2, Sentinels.NULL_SHORT, (short) 32000),
                Column.ofValues("SomeInts", 3, Sentinels.NULL_INT, 2000000000),
                Column.ofValues("SomeLongs", 4L, Sentinels.NULL_LONG, 4000000000L));

        invokeTest(defaultCsvReader().setParsers(Parsers.COMPLETE).setNullValueLiteral("NULL"), input, expected);
    }

    /**
     * Test column-specific null literals values which may be specified by column name or index, and also show that
     * Unicode characters work as the null literal.
     */
    @Test
    public void customColumnSpecificNullValue() throws CsvReaderException {
        final String input = "" +
                "SomeBytes,SomeShorts,SomeInts,SomeLongs\n" +
                "1,2,3,4\n" +
                "❌,🔥,⋰⋱,𝓓𝓮𝓮𝓹𝓱𝓪𝓿𝓮𝓷\n" +
                "100,32000,2000000000,4000000000\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("SomeBytes", (byte) 1, Sentinels.NULL_BYTE, (byte) 100),
                Column.ofValues("SomeShorts", (short) 2, Sentinels.NULL_SHORT, (short) 32000),
                Column.ofValues("SomeInts", 3, Sentinels.NULL_INT, 2000000000),
                Column.ofValues("SomeLongs", 4L, Sentinels.NULL_LONG, 4000000000L));

        invokeTest(defaultCsvReader()
                .setParsers(Parsers.COMPLETE)
                .setNullValueLiteralFor(1, "❌")
                .setNullValueLiteralFor(2, "🔥")
                .setNullValueLiteralFor("SomeInts", "⋰⋱")
                .setNullValueLiteralFor("SomeLongs", "𝓓𝓮𝓮𝓹𝓱𝓪𝓿𝓮𝓷"),
                input, expected);
    }

    /**
     * Provide a number of rows larger than ParserBase.DEST_BLOCK_SIZE.
     */
    @Test
    public void manyRows() throws CsvReaderException {
        final StringBuilder sb = new StringBuilder();
        sb.append(
                "SomeBooleans,SomeBytes,SomeShorts,SomeInts,SomeLongs,SomeDoubles,SomeStrings,SomeChars,SomeDateTimes,SomeTimestamps\n");
        final TByteArrayList booleans = new TByteArrayList();
        final TByteArrayList bytes = new TByteArrayList();
        final TShortArrayList shorts = new TShortArrayList();
        final TIntArrayList ints = new TIntArrayList();
        final TLongArrayList longs = new TLongArrayList();
        final TDoubleArrayList doubles = new TDoubleArrayList();
        final ArrayList<String> strings = new ArrayList<>();
        final TCharArrayList chars = new TCharArrayList();
        final TLongArrayList dateTimesAsLongs = new TLongArrayList();
        final TLongArrayList timestampsAsLongs = new TLongArrayList();
        final String qq = "qq";
        final long dtl = 799402088000000000L; // 1995-05-02 08:08:08Z
        final long tsl = 3456789012L;
        // Make sure we have a few more rows than Parser.DEST_BLOCK_SIZE
        for (int ii = 0; ii < Parser.CHUNK_SIZE + 3; ++ii) {
            sb.append("true,5,6,7,8,1.1,qq,r,1995-05-02 08:08:08Z,3456789012\n");
            booleans.add((byte) 1);
            bytes.add((byte) 5);
            shorts.add((short) 6);
            ints.add(7);
            longs.add(8);
            doubles.add(1.1);
            strings.add(qq);
            chars.add('r');
            dateTimesAsLongs.add(dtl);
            timestampsAsLongs.add(tsl);
        }
        // Add a row like this somewhere (let's put it at the end to make things challenging) so inference picks the
        // right types.
        sb.append("false,100,32000,2000000000,4000000000,6.6e50,yy,z,2020-03-05 12:34:56Z,123456789\n");
        booleans.add((byte) 0);
        bytes.add((byte) 100);
        shorts.add((short) 32000);
        ints.add(2000000000);
        longs.add(4000000000L);
        doubles.add(6.6e50);
        strings.add("yy");
        chars.add('z');
        dateTimesAsLongs.add(1583411696000000000L); // 2020-03-05 12:34:56Z
        timestampsAsLongs.add(123456789);

        final String input = sb.toString();
        final ColumnSet expected = ColumnSet.of(
                Column.ofArray("SomeBooleans", booleans.toArray()).reinterpret(boolean.class),
                Column.ofArray("SomeBytes", bytes.toArray()),
                Column.ofArray("SomeShorts", shorts.toArray()),
                Column.ofArray("SomeInts", ints.toArray()),
                Column.ofArray("SomeLongs", longs.toArray()),
                Column.ofArray("SomeDoubles", doubles.toArray()),
                Column.ofArray("SomeStrings", strings.toArray(new String[0])),
                Column.ofArray("SomeChars", chars.toArray()),
                Column.ofArray("SomeDateTimes", dateTimesAsLongs.toArray()).reinterpret(Instant.class),
                Column.ofArray("SomeTimestamps", timestampsAsLongs.toArray()).reinterpret(Instant.class));
        invokeTest(defaultCsvReader()
                .setParsers(Parsers.COMPLETE)
                .setParserFor("SomeTimestamps", Parsers.TIMESTAMP_NANOS),
                input, expected);
    }

    @Test
    public void customParser() throws CsvReaderException {
        final String bd1 =
                "81290897538197389132106321892137218932178913227138932178912312132.21879213278912387692138723198";
        final String bd2 =
                "-9210381027382193791312718239712389127812931236183167913268912683921681293621891236821.12986178632478123678312762318";

        final String input = "" +
                "Index,BigValues\n" +
                "0," + bd1 + "\n" +
                "1,\n" +
                "2," + bd2 + "\n";

        final ColumnSet expected = ColumnSet.of(
                Column.ofValues("Index", 0, 1, 2),
                Column.ofRefs("BigValues", new BigDecimal(bd1), null, new BigDecimal(bd2)));

        invokeTest(defaultCsvReader()
                .setParserFor(2, new MyBigDecimalParser()),
                input, expected);
    }

    private static class MyBigDecimalParser implements Parser<BigDecimal[]> {
        @NotNull
        @Override
        public ParserContext<BigDecimal[]> makeParserContext(GlobalContext gctx, int chunkSize) {
            final MyBigDecimalSink sink = new MyBigDecimalSink();
            return new ParserContext<>(sink, null, new BigDecimal[chunkSize]);
        }

        @Override
        public long tryParse(GlobalContext gctx, ParserContext<BigDecimal[]> pctx, IteratorHolder ih, long begin,
                long end, boolean appending) throws CsvReaderException {
            final boolean[] nulls = gctx.nullChunk();

            final Sink<BigDecimal[]> sink = pctx.sink();
            final BigDecimal[] values = pctx.valueChunk();

            // Reusable buffer
            char[] charData = new char[0];

            long current = begin;
            int chunkIndex = 0;
            do {
                if (chunkIndex == values.length) {
                    sink.write(values, nulls, current, current + chunkIndex, appending);
                    current += chunkIndex;
                    chunkIndex = 0;
                }
                if (current + chunkIndex == end) {
                    break;
                }
                if (gctx.isNullCell(ih)) {
                    nulls[chunkIndex++] = true;
                    continue;
                }
                final ByteSlice bs = ih.bs();
                if (!RangeTests.isAscii(bs.data(), bs.begin(), bs.end())) {
                    break;
                }

                // Convert bytes to chars. Annoying.
                if (charData.length < bs.size()) {
                    charData = new char[bs.size()];
                }
                int destIndex = 0;
                for (int cur = bs.begin(); cur != bs.end(); ++cur) {
                    charData[destIndex++] = (char) bs.data()[cur];
                }

                try {
                    values[chunkIndex] = new BigDecimal(charData, 0, destIndex);
                } catch (NumberFormatException ne) {
                    break;
                }
                nulls[chunkIndex] = false;
                ++chunkIndex;
            } while (ih.tryMoveNext());
            sink.write(values, nulls, current, current + chunkIndex, appending);
            return current + chunkIndex;
        }
    }

    private static class MyBigDecimalSink implements Sink<BigDecimal[]>, ColumnProvider<BigDecimal[]> {
        private final List<BigDecimal> dest = new ArrayList<>();

        @Override
        public void write(BigDecimal[] src, boolean[] isNull, long destBegin, long destEnd, boolean appending) {
            if (destBegin == destEnd) {
                return;
            }

            final int size = Math.toIntExact(destEnd - destBegin);
            if (appending) {
                // If the new area starts beyond the end of the destination, pad the destination.
                while (dest.size() < destBegin) {
                    dest.add(null);
                }
                for (int ii = 0; ii < size; ++ii) {
                    dest.add(isNull[ii] ? null : src[ii]);
                }
                return;
            }

            final int destBeginAsInt = Math.toIntExact(destBegin);
            for (int ii = 0; ii < size; ++ii) {
                dest.set(destBeginAsInt + ii, isNull[ii] ? null : src[ii]);
            }
        }

        @Override
        public Column<BigDecimal[]> toColumn(final String columnName) {
            return Column.ofArray(columnName, dest.toArray(new BigDecimal[0]));
        }
    }

    private static final class ColumnSet {
        public static final ColumnSet NONE = new ColumnSet(new Column[0], 0);

        private final Column<?>[] columns;
        private final int columnSize;

        public static ColumnSet of(Column<?>... columns) {
            if (columns.length == 0) {
                throw new RuntimeException("Empty column set is not permitted");
            }
            final int c0Size = columns[0].size();
            for (int ii = 1; ii < columns.length; ++ii) { // Deliberately starting at 1.
                final int ciiSize = columns[ii].size();
                if (ciiSize != c0Size) {
                    throw new RuntimeException(
                            String.format("Column %d (size %d) has a different size than column 0 (size %d)",
                                    ii, ciiSize, c0Size));
                }
            }
            return new ColumnSet(columns, c0Size);
        }

        private ColumnSet(Column<?>[] columns, int columnSize) {
            this.columns = columns;
            this.columnSize = columnSize;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            final List<Column<?>> colList = List.of(columns);

            final BiFunction<Class<?>, Class<?>, String> renderType = (etype, rtype) -> {
                if (etype == rtype) {
                    return etype.getCanonicalName();
                }
                return etype.getCanonicalName() + "->" + rtype.getCanonicalName();
            };

            Renderer.renderList(sb, colList, ",",
                    col -> String.format("%s(%s)", col.name(),
                            renderType.apply(col.elementType(), col.reinterpretedType)));
            for (int jj = 0; jj < columnSize; ++jj) {
                final int jjFinal = jj;
                sb.append('\n');
                Renderer.renderList(sb, colList, ",", col -> safeToString(col.getItem(jjFinal)));
            }
            return sb.toString();
        }

        private static String safeToString(Object o) {
            return o == null ? "(null)" : o.toString();
        }
    }

    private static final class Column<TARRAY> {
        private final String name;
        private final TARRAY values;
        private final int size;
        private final Class<?> reinterpretedType;

        public static Column<byte[]> ofValues(final String name, final byte... values) {
            return ofArray(name, values);
        }

        public static Column<short[]> ofValues(final String name, final short... values) {
            return ofArray(name, values);
        }

        public static Column<int[]> ofValues(final String name, final int... values) {
            return ofArray(name, values);
        }

        public static Column<long[]> ofValues(final String name, final long... values) {
            return ofArray(name, values);
        }

        public static Column<float[]> ofValues(final String name, final float... values) {
            return ofArray(name, values);
        }

        public static Column<double[]> ofValues(final String name, final double... values) {
            return ofArray(name, values);
        }

        public static Column<char[]> ofValues(final String name, final char... values) {
            return ofArray(name, values);
        }

        public static <T> Column<T[]> ofRefs(final String name, final T... values) {
            return ofArray(name, values);
        }

        public static <TARRAY> Column<TARRAY> ofArray(final String name, final TARRAY values) {
            return new Column<>(name, values);
        }

        private Column(final String name, final TARRAY values) {
            this(name, values, Array.getLength(values), values.getClass().getComponentType());
        }

        private Column(final String name, final TARRAY values, int size, Class<?> reinterpretedType) {
            this.name = name;
            this.values = values;
            this.size = size;
            this.reinterpretedType = reinterpretedType;
        }

        public Column<TARRAY> reinterpret(Class<?> reinterpretedType) {
            return new Column<>(name, values, size, reinterpretedType);
        }

        public int size() {
            return size;
        }

        public String name() {
            return name;
        }

        public Class<?> elementType() {
            return values.getClass().getComponentType();
        }

        public Class<?> reinterpretedType() {
            return reinterpretedType;
        }

        public Object getItem(int index) {
            return Array.get(values, index);
        }
    }

    private static CsvReader defaultCsvReader() {
        return new CsvReader().setIgnoreSurroundingSpaces(true);
    }

    private static void invokeTest(CsvReader csvReader, String input, ColumnSet expected) throws CsvReaderException {
        final ColumnSet actual = parse(csvReader, input);
        final String expectedToString = expected.toString();
        final String actualToString = actual.toString();
        Assertions.assertThat(actualToString).isEqualTo(expectedToString);
    }

    private static ColumnSet parse(CsvReader csvReader, String input) throws CsvReaderException {
        final StringReader reader = new StringReader(input);
        final ReaderInputStream inputStream = new ReaderInputStream(reader, StandardCharsets.UTF_8);
        return parse(csvReader, inputStream);
    }

    /**
     * Parses {@code reader} according to the specifications of {@code this}. The {@code reader} will be closed upon
     * return.
     *
     * <p>
     * Note: this implementation will buffer the {@code reader} internally.
     *
     * @param inputStream the input stream.
     * @return the new table
     * @throws CsvReaderException If any sort of failure occurs.
     */
    private static ColumnSet parse(CsvReader csvReader, InputStream inputStream) throws CsvReaderException {
        final CsvReader.Result result = csvReader.read(inputStream, makeMySinkFactory());

        final int numCols = result.numCols();

        final String[] columnNames = result.columnNames();
        final Sink<?>[] sinks = result.columns();
        final Column<?>[] columns = new Column[numCols];
        for (int ii = 0; ii < numCols; ++ii) {
            final String columnName = columnNames[ii];
            final ColumnProvider<?> sink = (ColumnProvider<?>) sinks[ii];
            columns[ii] = sink.toColumn(columnName);
        }
        return ColumnSet.of(columns);
    }

    public interface ColumnProvider<TARRAY> {
        Column<TARRAY> toColumn(final String columnName);
    }

    private static abstract class MySinkBase<TCOLLECTION, TARRAY> implements Sink<TARRAY>, ColumnProvider<TARRAY> {
        protected final TCOLLECTION collection;
        protected int collectionSize;
        protected final FillOperation<TCOLLECTION> fillOperation;
        protected final SetOperation<TCOLLECTION, TARRAY> setOperation;
        protected final AddOperation<TCOLLECTION, TARRAY> addOperation;
        protected final BiFunction<TCOLLECTION, String, Column<TARRAY>> toColumnOperation;

        protected MySinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                BiFunction<TCOLLECTION, String, Column<TARRAY>> toColumnOperation) {
            this.collection = collection;
            this.fillOperation = fillOperation;
            this.setOperation = setOperation;
            this.addOperation = addOperation;
            this.toColumnOperation = toColumnOperation;
        }

        @Override
        public final void write(final TARRAY src, final boolean[] isNull, final long destBegin,
                final long destEnd, boolean appending) {
            if (destBegin == destEnd) {
                return;
            }
            final int size = Math.toIntExact(destEnd - destBegin);
            final int destBeginAsInt = Math.toIntExact(destBegin);
            final int destEndAsInt = Math.toIntExact(destEnd);
            nullFlagsToValues(isNull, src, size);

            if (!appending) {
                // Replacing.
                setOperation.apply(collection, destBeginAsInt, src, 0, size);
                return;
            }

            // Appending. First, if the new area starts beyond the end of the destination, pad the destination.
            if (collectionSize < destBegin) {
                fillOperation.apply(collection, collectionSize, destBeginAsInt);
                collectionSize = destBeginAsInt;
            }
            // Then do the append.
            addOperation.apply(collection, src, 0, size);
            collectionSize = destEndAsInt;
        }

        protected abstract void nullFlagsToValues(final boolean[] isNull, final TARRAY values, final int size);

        public final Column<TARRAY> toColumn(final String columnName) {
            return toColumnOperation.apply(collection, columnName);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.fill(int fromIndex, int toIndex, 0.0)
         */
        protected interface FillOperation<TCOLLECTION> {
            void apply(TCOLLECTION coll, int fromIndex, int toIndex);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.set(int offset, double[] values, int valOffset, int length)
         */
        protected interface SetOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, int offset, TARRAY values, int vallOffset, int length);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.add(double[] values, int offset, int length)
         */
        protected interface AddOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, TARRAY values, int offset, int length);
        }
    }

    private static abstract class MySourceAndSinkBase<TCOLLECTION, TARRAY> extends MySinkBase<TCOLLECTION, TARRAY>
            implements io.deephaven.csv.sinks.Source<TARRAY>, Sink<TARRAY> {
        private final ToArrayOperation<TCOLLECTION, TARRAY> toArrayOperation;

        protected MySourceAndSinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                BiFunction<TCOLLECTION, String, Column<TARRAY>> toColumnOperation,
                ToArrayOperation<TCOLLECTION, TARRAY> toArrayOperation) {
            super(collection, fillOperation, setOperation, addOperation, toColumnOperation);
            this.toArrayOperation = toArrayOperation;
        }

        @Override
        public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            if (srcBegin == srcEnd) {
                return;
            }
            final int size = Math.toIntExact(srcEnd - srcBegin);
            toArrayOperation.apply(collection, dest, Math.toIntExact(srcBegin), 0, size);
            nullValuesToFlags(dest, isNull, size);
        }

        protected abstract void nullValuesToFlags(final TARRAY values, final boolean[] isNull, final int size);

        /**
         * Meant to be paired with e.g. TDoubleArrayList.add(double[] dest, int source_pos, int dest_pos, int length)
         */
        private interface ToArrayOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, TARRAY dest, int source_pos_, int dest_pos, int length);
        }
    }

    private static class MyByteSinkBase extends MySourceAndSinkBase<TByteArrayList, byte[]> {
        protected final byte nullSentinel;

        public MyByteSinkBase(final byte nullSentinel, final Class<?> reinterpretedType) {
            super(new TByteArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (byte) 0),
                    TByteArrayList::set,
                    TByteArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()).reinterpret(reinterpretedType),
                    TByteArrayList::toArray);
            this.nullSentinel = nullSentinel;
        }

        @Override
        protected final void nullFlagsToValues(boolean[] isNull, byte[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = nullSentinel;
                }
            }
        }

        @Override
        protected final void nullValuesToFlags(byte[] values, boolean[] isNull, int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == nullSentinel;
            }
        }
    }

    private static final class MyByteSink extends MyByteSinkBase {
        public MyByteSink() {
            super(Sentinels.NULL_BYTE, byte.class);
        }
    }

    private static final class MyShortSink extends MySourceAndSinkBase<TShortArrayList, short[]> {
        public MyShortSink() {
            super(new TShortArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (short) 0),
                    TShortArrayList::set,
                    TShortArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()),
                    TShortArrayList::toArray);
        }

        @Override
        protected void nullFlagsToValues(boolean[] isNull, short[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = Sentinels.NULL_SHORT;
                }
            }
        }

        @Override
        protected void nullValuesToFlags(short[] values, boolean[] isNull, int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == Sentinels.NULL_SHORT;
            }
        }
    }

    private static final class MyIntSink extends MySourceAndSinkBase<TIntArrayList, int[]> {
        public MyIntSink() {
            super(new TIntArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TIntArrayList::set,
                    TIntArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()),
                    TIntArrayList::toArray);
        }

        @Override
        protected void nullFlagsToValues(boolean[] isNull, int[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = Sentinels.NULL_INT;
                }
            }
        }

        @Override
        protected void nullValuesToFlags(int[] values, boolean[] isNull, int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == Sentinels.NULL_INT;
            }
        }
    }

    private static class MyLongSinkBase extends MySourceAndSinkBase<TLongArrayList, long[]> {
        private final long nullSentinel;

        public MyLongSinkBase(final long nullSentinel, final Class<?> reinterpretedType) {
            super(new TLongArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0L),
                    TLongArrayList::set,
                    TLongArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()).reinterpret(reinterpretedType),
                    TLongArrayList::toArray);
            this.nullSentinel = nullSentinel;
        }

        @Override
        protected final void nullFlagsToValues(boolean[] isNull, long[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = nullSentinel;
                }
            }
        }

        @Override
        protected final void nullValuesToFlags(long[] values, boolean[] isNull, int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == nullSentinel;
            }
        }
    }

    private static final class MyLongSink extends MyLongSinkBase {
        public MyLongSink() {
            super(Sentinels.NULL_LONG, long.class);
        }
    }

    private static final class MyFloatSink extends MySinkBase<TFloatArrayList, float[]> {
        public MyFloatSink() {
            super(new TFloatArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TFloatArrayList::set,
                    TFloatArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()));
        }

        @Override
        protected void nullFlagsToValues(boolean[] isNull, float[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = Sentinels.NULL_FLOAT;
                }
            }
        }
    }

    private static final class MyDoubleSink extends MySinkBase<TDoubleArrayList, double[]> {
        public MyDoubleSink() {
            super(new TDoubleArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TDoubleArrayList::set,
                    TDoubleArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()));
        }

        @Override
        protected void nullFlagsToValues(boolean[] isNull, double[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = Sentinels.NULL_DOUBLE;
                }
            }
        }
    }

    private static final class MyBooleanAsByteSink extends MyByteSinkBase {
        public MyBooleanAsByteSink() {
            super(Sentinels.NULL_BOOLEAN_AS_BYTE, boolean.class);
        }
    }

    private static final class MyCharSink extends MySinkBase<TCharArrayList, char[]> {
        public MyCharSink() {
            super(new TCharArrayList(),
                    (coll, from, to) -> coll.fill(from, to, (char) 0),
                    TCharArrayList::set,
                    TCharArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()));
        }

        @Override
        protected void nullFlagsToValues(boolean[] isNull, char[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = Sentinels.NULL_CHAR;
                }
            }
        }
    }

    private static final class MyStringSink extends MySinkBase<ArrayList<String>, String[]> {
        public MyStringSink() {
            super(new ArrayList<>(),
                    MyStringSink::fill,
                    MyStringSink::set,
                    MyStringSink::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray(new String[0])));
        }

        @Override
        protected void nullFlagsToValues(boolean[] isNull, String[] values, int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = null;
                }
            }
        }

        private static void fill(final ArrayList<String> dest, final int from, final int to) {
            for (int current = from; current != to; ++current) {
                if (current < dest.size()) {
                    dest.set(current, null);
                } else {
                    dest.add(null);
                }
            }
        }

        private static void set(final ArrayList<String> dest, final int destOffset, final String[] src,
                final int srcOffset, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                dest.set(destOffset + ii, src[srcOffset + ii]);
            }
        }

        private static void add(final ArrayList<String> dest, final String[] src, final int srcOffset,
                final int size) {
            for (int ii = 0; ii < size; ++ii) {
                dest.add(src[srcOffset + ii]);
            }
        }
    }

    private static final class MyDateTimeAsLongSink extends MyLongSinkBase {
        public MyDateTimeAsLongSink() {
            super(Sentinels.NULL_DATETIME_AS_LONG, Instant.class);
        }
    }

    private static final class MyTimestampAsLongSink extends MyLongSinkBase {
        public MyTimestampAsLongSink() {
            super(Sentinels.NULL_TIMESTAMP_AS_LONG, Instant.class);
        }
    }

    private static SinkFactory makeMySinkFactory() {
        return SinkFactory.of(
                MyByteSink::new, Sentinels.NULL_BYTE,
                MyShortSink::new, Sentinels.NULL_SHORT,
                MyIntSink::new, Sentinels.NULL_INT,
                MyLongSink::new, Sentinels.NULL_LONG,
                MyFloatSink::new, Sentinels.NULL_FLOAT,
                MyDoubleSink::new, Sentinels.NULL_DOUBLE,
                MyBooleanAsByteSink::new,
                MyCharSink::new, Sentinels.NULL_CHAR,
                MyStringSink::new, null,
                MyDateTimeAsLongSink::new, Sentinels.NULL_LONG,
                MyTimestampAsLongSink::new, Sentinels.NULL_LONG);
    }
}

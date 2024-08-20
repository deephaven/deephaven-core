//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.csv;

import io.deephaven.base.FileUtils;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

import static io.deephaven.time.DateTimeUtils.*;
import static io.deephaven.util.QueryConstants.*;

/**
 * Unit tests for {@link CsvTools}.
 */
@Category({OutOfBandTest.class})
public class TestCsvTools {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private File tmpDir;

    @Before
    public void createDir() throws IOException {
        tmpDir = Files.createTempDirectory("TestCsvTools-").toFile();
    }

    @After
    public void removeDir() {
        FileUtils.deleteRecursively(tmpDir);
    }

    @Test
    public void testTableDividendsCSV() throws CsvReaderException {
        final String fileDividends = "Sym,Type,Price,SecurityId\n" +
                "GOOG, Dividend, 0.25, 200\n" +
                "T, Dividend, 0.15, 300\n" +
                " Z, Dividend, 0.18, 500";
        Table tableDividends = CsvTools.readCsv(new ByteArrayInputStream(fileDividends.getBytes()));
        Assert.assertEquals(3, tableDividends.size());
        Assert.assertEquals(4, tableDividends.meta().size());
        Assert.assertEquals(0.15, tableDividends.getColumnSource("Price").getDouble(tableDividends.getRowSet().get(1)),
                0.000001);
        Assert.assertEquals(300,
                tableDividends.getColumnSource("SecurityId").getInt(tableDividends.getRowSet().get(1)));
        Assert.assertEquals("Z", tableDividends.getColumnSource("Sym").get(tableDividends.getRowSet().get(2)));
    }

    @Test
    public void testTableDividendsCSVNoTrim() throws CsvReaderException {
        final String fileDividends = "Sym,Type,Price,SecurityId\n" +
                "GOOG, Dividend, 0.25, 200\n" +
                "T, Dividend, 0.15, 300\n" +
                " Z, Dividend, 0.18, 500";
        Table tableDividends = CsvTools.readCsv(new ByteArrayInputStream(fileDividends.getBytes()), "DEFAULT");
        Assert.assertEquals(3, tableDividends.size());
        Assert.assertEquals(4, tableDividends.meta().size());
        Assert.assertEquals(0.15, tableDividends.getColumnSource("Price").getDouble(tableDividends.getRowSet().get(1)),
                0.000001);
        Assert.assertEquals(300,
                tableDividends.getColumnSource("SecurityId").getInt(tableDividends.getRowSet().get(1)));
        Assert.assertEquals(" Z", tableDividends.getColumnSource("Sym").get(tableDividends.getRowSet().get(2)));
    }

    @Test
    public void testCompressedCSV() throws IOException, CsvReaderException {
        final String contents = "A,B,C,D\n"
                + "\"Hello World\",3.0,5,700\n"
                + "\"Goodbye Cruel World\",3.1,1000000,800\n"
                + "\"Hello World Again!\",4.0,20000000000,900\n";
        final byte[] contentBytes = contents.getBytes(StandardCharsets.UTF_8);
        final byte[] contentTarBytes;
        try (final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                final TarArchiveOutputStream tarOut = new TarArchiveOutputStream(bytesOut)) {
            final TarArchiveEntry tarEntry = new TarArchiveEntry("test.csv");
            tarEntry.setSize(contentBytes.length);
            tarOut.putArchiveEntry(tarEntry);
            tarOut.write(contentBytes);
            tarOut.closeArchiveEntry();
            tarOut.flush();
            contentTarBytes = bytesOut.toByteArray();
        }
        final File csvFile = new File(tmpDir, "test.csv");
        final File csvTarFile = new File(tmpDir, "test.csv.tar");
        Files.write(csvFile.toPath(), contentBytes);
        Files.write(csvTarFile.toPath(), contentTarBytes);
        final Table expected = CsvTools.readCsv(csvFile.toPath());
        final Table actual = CsvTools.readCsv(csvTarFile.toPath());
        TstUtils.assertTableEquals(expected, actual);
    }

    @Test
    public void testUncompressedCSVFromPath() throws IOException, CsvReaderException {
        String contents = "A,B,C,D\n"
                + "\"Hello World\",3.0,5,700\n"
                + "\"Goodbye Cruel World\",3.1,1000000,800\n"
                + "\"Hello World Again!\",4.0,20000000000,900\n";

        // Although this seems arbitrary, we want to make sure our file is large enough to possibly be a tar
        while (contents.length() < TarConstants.DEFAULT_RCDSIZE) {
            contents += contents;
        }
        final byte[] contentBytes = contents.getBytes(StandardCharsets.UTF_8);

        final File csvFile = new File(tmpDir, "test.csv");
        Files.write(csvFile.toPath(), contentBytes);

        final Table expected = CsvTools.readCsv(csvFile.toPath());
        final Table actual = CsvTools.readCsv(csvFile.getPath());
        TstUtils.assertTableEquals(expected, actual);
    }

    @Test
    public void testLoadCsv() throws Exception {
        final String allSeparators = ",|\tzZ- 9@";
        System.out.println("Char Set: " + Charset.defaultCharset().displayName());

        for (char separator : allSeparators.toCharArray()) {
            ByteArrayOutputStream ba = new ByteArrayOutputStream();

            PrintWriter out = new PrintWriter(ba);

            out.printf("colA%scolB%scolC%scolD%scolE%scolF%scolG%n", separator, separator, separator, separator,
                    separator, separator);
            out.printf("\"mark1%smark2\"%s1%s1%s1%s%s%strue%n", separator, separator, separator, separator,
                    separator, separator, separator);
            out.printf("etti%s3%s6%s2%s%s%sFALSE%n", separator, separator, separator, separator, separator,
                    separator);
            out.printf("%s%s%s%s%s%s%n", separator, separator, separator, separator,
                    separator, separator);
            out.printf("%s%s%s%s%s%s%n", separator, separator, separator, separator, separator, separator);
            out.printf("test%s3%s7.0%stest%s%s%sTRUE%n", separator, separator, separator, separator, separator,
                    separator);

            out.flush();
            out.close();

            System.out.println("=============================");
            System.out.println(ba);
            System.out.println("Separator: '" + separator + "'");
            Table table = CsvTools.readCsv(new ByteArrayInputStream(ba.toByteArray()), separator);

            TableDefinition definition = table.getDefinition();

            Assert.assertEquals("colA", definition.getColumns().get(0).getName());
            Assert.assertEquals(String.class, definition.getColumns().get(0).getDataType());

            Assert.assertEquals("colB", definition.getColumns().get(1).getName());
            Assert.assertEquals(int.class, definition.getColumns().get(1).getDataType());

            Assert.assertEquals("colC", definition.getColumns().get(2).getName());
            Assert.assertEquals(double.class, definition.getColumns().get(2).getDataType());

            Assert.assertEquals("colD", definition.getColumns().get(3).getName());
            Assert.assertEquals(String.class, definition.getColumns().get(3).getDataType());

            Assert.assertEquals("colE", definition.getColumns().get(4).getName());
            Assert.assertEquals(String.class, definition.getColumns().get(4).getDataType());

            Assert.assertEquals("colF", definition.getColumns().get(5).getName());
            Assert.assertEquals(String.class, definition.getColumns().get(5).getDataType());

            Assert.assertEquals("colG", definition.getColumns().get(6).getName());
            Assert.assertEquals(Boolean.class, definition.getColumns().get(6).getDataType());

            Assert.assertEquals(String.format("mark1%smark2", separator),
                    table.getColumnSource("colA").get(table.getRowSet().get(0)));
            Assert.assertEquals(1, table.getColumnSource("colB").getInt(table.getRowSet().get(0)));
            Assert.assertEquals(1.0, table.getColumnSource("colC").getDouble(table.getRowSet().get(0)), 0.000001);
            Assert.assertEquals("1", table.getColumnSource("colD").get(table.getRowSet().get(0)));
            Assert.assertNull(table.getColumnSource("colE").get(table.getRowSet().get(0)));
            Assert.assertNull(table.getColumnSource("colF").get(table.getRowSet().get(0)));
            Assert.assertEquals(Boolean.TRUE, table.getColumnSource("colG").getBoolean(table.getRowSet().get(0)));

            Assert.assertNull(table.getColumnSource("colA").get(table.getRowSet().get(2)));
            Assert.assertEquals(QueryConstants.NULL_INT,
                    table.getColumnSource("colB").getInt(table.getRowSet().get(2)));
            Assert.assertEquals(QueryConstants.NULL_DOUBLE,
                    table.getColumnSource("colC").getDouble(table.getRowSet().get(2)),
                    0.0000001);
            Assert.assertNull(table.getColumnSource("colD").get(table.getRowSet().get(2)));
            Assert.assertNull(table.getColumnSource("colE").get(table.getRowSet().get(2)));
            Assert.assertNull(table.getColumnSource("colF").get(table.getRowSet().get(2)));
            Assert.assertEquals(QueryConstants.NULL_BOOLEAN,
                    table.getColumnSource("colG").getBoolean(table.getRowSet().get(2)));
        }
    }

    @Test
    public void testWriteCsv() throws Exception {
        final File csvFile = new File(tmpDir, "tmp.csv");
        final String[] colNames = {"Strings", "Chars", "Bytes", "Shorts", "Ints", "Longs", "Floats", "Doubles",
                "Instants", "ZonedDateTimes", "Booleans"};
        final Table tableToTest = new InMemoryTable(
                colNames,
                new Object[] {
                        new String[] {
                                "key11", "key11", "key21", "key21", "key22", null, "ABCDEFGHIJK", "\"", "123",
                                "456", "789", ",", "8"
                        },
                        new char[] {
                                'a', 'b', 'b', NULL_CHAR, 'c', '\n', ',', MIN_CHAR, MAX_CHAR, 'Z', 'Y', '~', '0'
                        },
                        new byte[] {
                                1, 2, 2, NULL_BYTE, 3, -99, -100, MIN_BYTE, MAX_BYTE, 5, 6, 7, 8
                        },
                        new short[] {
                                1, 2, 2, NULL_SHORT, 3, -99, -100, MIN_SHORT, MAX_SHORT, 5, 6, 7, 8
                        },
                        new int[] {
                                1, 2, 2, NULL_INT, 3, -99, -100, MIN_INT, MAX_INT, 5, 6, 7, 8
                        },
                        new long[] {
                                1, 2, 2, NULL_LONG, 3, -99, -100, MIN_LONG, MAX_LONG, 5, 6, 7, 8
                        },
                        new float[] {
                                2.342f, 0.0932f, 10000000, NULL_FLOAT, 3, MIN_FINITE_FLOAT, MAX_FINITE_FLOAT,
                                Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, -1.00f, 0.0f, -0.001f, Float.NaN
                        },
                        new double[] {
                                2.342, 0.0932, 10000000, NULL_DOUBLE, 3, MIN_FINITE_DOUBLE, MAX_FINITE_DOUBLE,
                                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -1.00, 0.0, -0.001, Double.NaN
                        },
                        new Instant[] {
                                epochNanosToInstant(100),
                                epochNanosToInstant(10000),
                                null,
                                epochNanosToInstant(100000),
                                epochNanosToInstant(1000000),
                                parseInstant("2022-11-06T02:00:00.000000000-04:00"),
                                parseInstant("2022-11-06T02:00:00.000000000-05:00"),
                                parseInstant("2022-11-06T02:00:01.000000001-04:00"),
                                parseInstant("2022-11-06T02:00:01.000000001-05:00"),
                                parseInstant("2022-11-06T02:59:59.999999999-04:00"),
                                parseInstant("2022-11-06T02:59:59.999999999-05:00"),
                                parseInstant("2022-11-06T03:00:00.000000000-04:00"),
                                parseInstant("2022-11-06T03:00:00.000000000-05:00")
                        },
                        new ZonedDateTime[] {
                                epochNanosToZonedDateTime(100, timeZone("America/New_York")),
                                epochNanosToZonedDateTime(10000, timeZone("America/New_York")),
                                null,
                                epochNanosToZonedDateTime(100000, timeZone("America/New_York")),
                                epochNanosToZonedDateTime(1000000, timeZone("America/New_York")),
                                parseZonedDateTime("2022-11-06T02:00:00.000000000 America/New_York"),
                                parseZonedDateTime("2022-11-06T02:00:00.000000000 America/New_York"),
                                parseZonedDateTime("2022-11-06T02:00:01.000000001 America/New_York"),
                                parseZonedDateTime("2022-11-06T02:00:01.000000001 America/New_York"),
                                parseZonedDateTime("2022-11-06T02:59:59.999999999 America/New_York"),
                                parseZonedDateTime("2022-11-06T02:59:59.999999999 America/New_York"),
                                parseZonedDateTime("2022-11-06T03:00:00.000000000 America/New_York"),
                                parseZonedDateTime("2022-11-06T03:00:00.000000000 America/New_York")
                        },
                        new Boolean[] {
                                null, false, true, true, false, false, false, false, true, false, null, null, null
                        }
                });
        final String[] casts = {
                "Bytes = (byte) Bytes", "Shorts = (short) Shorts", "Floats = (float) Floats",
                "ZonedDateTimes = toZonedDateTime(ZonedDateTimes, 'America/New_York')"};
        final String allSeparators = ",|\tzZ- 90@";
        for (final char separator : allSeparators.toCharArray()) {
            for (final boolean nullAsEmpty : new boolean[] {false, true}) {
                CsvTools.writeCsv(
                        tableToTest, csvFile.getPath(), false, timeZone(), nullAsEmpty, separator, colNames);
                final Table result = CsvTools.readCsv(csvFile.getPath(), CsvSpecs.builder()
                        .delimiter(separator)
                        .nullValueLiterals(List.of(nullAsEmpty ? "" : "(null)"))
                        .build());
                TstUtils.assertTableEquals(tableToTest, result.updateView(casts));
            }
        }
    }
}

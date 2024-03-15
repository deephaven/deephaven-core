//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.csv;

import io.deephaven.base.FileUtils;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.DataAccessHelpers;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
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
import java.util.List;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;

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
        Assert.assertEquals(0.15, DataAccessHelpers.getColumn(tableDividends, 2).getDouble(1), 0.000001);
        Assert.assertEquals(300, DataAccessHelpers.getColumn(tableDividends, 3).getInt(1));
        Assert.assertEquals("Z", DataAccessHelpers.getColumn(tableDividends, 0).get(2));
    }

    @Test
    public void testTableDividendsCSVNoTrim() throws CsvReaderException {
        final String fileDividends = "Sym,Type,Price,SecurityId\n" +
                "GOOG, Dividend, 0.25, 200\n" +
                "T, Dividend, 0.15, 300\n" +
                " Z, Dividend, 0.18, 500";
        Table tableDividends = CsvTools
                .readCsv(new ByteArrayInputStream(fileDividends.getBytes()), "DEFAULT");
        Assert.assertEquals(3, tableDividends.size());
        Assert.assertEquals(4, tableDividends.meta().size());
        Assert.assertEquals(0.15, DataAccessHelpers.getColumn(tableDividends, 2).get(1));
        Assert.assertEquals(300, DataAccessHelpers.getColumn(tableDividends, 3).get(1));
        Assert.assertEquals(" Z", DataAccessHelpers.getColumn(tableDividends, 0).get(2));
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
                    DataAccessHelpers.getColumn(table, "colA").get(0));
            Assert.assertEquals(1, DataAccessHelpers.getColumn(table, "colB").getInt(0));
            Assert.assertEquals(1.0, DataAccessHelpers.getColumn(table, "colC").getDouble(0), 0.000001);
            Assert.assertEquals("1", DataAccessHelpers.getColumn(table, "colD").get(0));
            Assert.assertNull(DataAccessHelpers.getColumn(table, "colE").get(0));
            Assert.assertNull(DataAccessHelpers.getColumn(table, "colF").get(0));
            Assert.assertEquals(Boolean.TRUE, DataAccessHelpers.getColumn(table, "colG").getBoolean(0));

            Assert.assertNull(DataAccessHelpers.getColumn(table, "colA").get(2));
            Assert.assertEquals(QueryConstants.NULL_INT, DataAccessHelpers.getColumn(table, "colB").getInt(2));
            Assert.assertEquals(QueryConstants.NULL_DOUBLE, DataAccessHelpers.getColumn(table, "colC").getDouble(2),
                    0.0000001);
            Assert.assertNull(DataAccessHelpers.getColumn(table, "colD").get(2));
            Assert.assertNull(DataAccessHelpers.getColumn(table, "colE").get(2));
            Assert.assertNull(DataAccessHelpers.getColumn(table, "colF").get(2));
            Assert.assertEquals(QueryConstants.NULL_BOOLEAN, DataAccessHelpers.getColumn(table, "colG").getBoolean(2));
        }
    }

    @Test
    public void testWriteCsv() throws Exception {
        final File csvFile = new File(tmpDir, "tmp.csv");
        final String[] colNames = {"StringKeys", "GroupedInts", "Doubles", "DateTime"};
        final long numCols = colNames.length;
        final Table tableToTest = new InMemoryTable(
                colNames,
                new Object[] {
                        new String[] {
                                "key11", "key11", "key21", "key21", "key22", null, "ABCDEFGHIJK", "\"", "123",
                                "456", "789", ",", "8"
                        },
                        new int[] {
                                1, 2, 2, NULL_INT, 3, -99, -100, Integer.MIN_VALUE + 1, Integer.MAX_VALUE,
                                5, 6, 7, 8
                        },
                        new double[] {
                                2.342, 0.0932, 10000000, NULL_DOUBLE, 3, Double.MIN_VALUE, Double.MAX_VALUE,
                                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -1.00, 0.0, -0.001, Double.NaN
                        },
                        new Instant[] {
                                DateTimeUtils.epochNanosToInstant(100),
                                DateTimeUtils.epochNanosToInstant(10000),
                                null,
                                DateTimeUtils.epochNanosToInstant(100000),
                                DateTimeUtils.epochNanosToInstant(1000000),
                                DateTimeUtils.parseInstant("2022-11-06T02:00:00.000000000-04:00"),
                                DateTimeUtils.parseInstant("2022-11-06T02:00:00.000000000-05:00"),
                                DateTimeUtils.parseInstant("2022-11-06T02:00:01.000000001-04:00"),
                                DateTimeUtils.parseInstant("2022-11-06T02:00:01.000000001-05:00"),
                                DateTimeUtils.parseInstant("2022-11-06T02:59:59.999999999-04:00"),
                                DateTimeUtils.parseInstant("2022-11-06T02:59:59.999999999-05:00"),
                                DateTimeUtils.parseInstant("2022-11-06T03:00:00.000000000-04:00"),
                                DateTimeUtils.parseInstant("2022-11-06T03:00:00.000000000-05:00")
                        }
                });

        final String allSeparators = ",|\tzZ- 9@";
        for (final char separator : allSeparators.toCharArray()) {
            CsvTools.writeCsv(
                    tableToTest, csvFile.getPath(), false, DateTimeUtils.timeZone(), false, separator, colNames);
            final Table result = CsvTools.readCsv(csvFile.getPath(),
                    CsvSpecs.builder().delimiter(separator).nullValueLiterals(List.of("(null)")).build());
            TstUtils.assertTableEquals(tableToTest, result);
        }
    }
}

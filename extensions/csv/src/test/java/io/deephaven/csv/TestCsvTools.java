package io.deephaven.csv;

import io.deephaven.base.FileUtils;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.time.DateTime;
import io.deephaven.time.TimeZone;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Scanner;
import java.util.regex.Pattern;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Unit tests for {@link CsvTools}.
 */
@Category({OutOfBandTest.class})
public class TestCsvTools {

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
        Assert.assertEquals(4, tableDividends.getMeta().size());
        Assert.assertEquals(0.15, tableDividends.getColumn(2).getDouble(1), 0.000001);
        Assert.assertEquals(300, tableDividends.getColumn(3).getInt(1));
        Assert.assertEquals("Z", tableDividends.getColumn(0).get(2));
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
        Assert.assertEquals(4, tableDividends.getMeta().size());
        Assert.assertEquals(0.15, tableDividends.getColumn(2).get(1));
        Assert.assertEquals(300, tableDividends.getColumn(3).get(1));
        Assert.assertEquals(" Z", tableDividends.getColumn(0).get(2));
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

            Assert.assertEquals("colA", definition.getColumnList().get(0).getName());
            Assert.assertEquals(String.class, definition.getColumnList().get(0).getDataType());

            Assert.assertEquals("colB", definition.getColumnList().get(1).getName());
            Assert.assertEquals(int.class, definition.getColumnList().get(1).getDataType());

            Assert.assertEquals("colC", definition.getColumnList().get(2).getName());
            Assert.assertEquals(double.class, definition.getColumnList().get(2).getDataType());

            Assert.assertEquals("colD", definition.getColumnList().get(3).getName());
            Assert.assertEquals(String.class, definition.getColumnList().get(3).getDataType());

            Assert.assertEquals("colE", definition.getColumnList().get(4).getName());
            Assert.assertEquals(String.class, definition.getColumnList().get(4).getDataType());

            Assert.assertEquals("colF", definition.getColumnList().get(5).getName());
            Assert.assertEquals(String.class, definition.getColumnList().get(5).getDataType());

            Assert.assertEquals("colG", definition.getColumnList().get(6).getName());
            Assert.assertEquals(Boolean.class, definition.getColumnList().get(6).getDataType());

            Assert.assertEquals(String.format("mark1%smark2", separator), table.getColumn("colA").get(0));
            Assert.assertEquals(1, table.getColumn("colB").getInt(0));
            Assert.assertEquals(1.0, table.getColumn("colC").getDouble(0), 0.000001);
            Assert.assertEquals("1", table.getColumn("colD").get(0));
            Assert.assertNull(table.getColumn("colE").get(0));
            Assert.assertNull(table.getColumn("colF").get(0));
            Assert.assertEquals(Boolean.TRUE, table.getColumn("colG").getBoolean(0));

            Assert.assertNull(table.getColumn("colA").get(2));
            Assert.assertEquals(QueryConstants.NULL_INT, table.getColumn("colB").getInt(2));
            Assert.assertEquals(QueryConstants.NULL_DOUBLE, table.getColumn("colC").getDouble(2), 0.0000001);
            Assert.assertNull(table.getColumn("colD").get(2));
            Assert.assertNull(table.getColumn("colE").get(2));
            Assert.assertNull(table.getColumn("colF").get(2));
            Assert.assertEquals(QueryConstants.NULL_BOOLEAN, table.getColumn("colG").getBoolean(2));
        }
    }

    @Test
    public void testWriteCsv() throws Exception {
        File csvFile = new File(tmpDir, "tmp.csv");
        String[] colNames = {"StringKeys", "GroupedInts", "Doubles", "DateTime"};
        long numCols = colNames.length;
        Table tableToTest = new InMemoryTable(
                colNames,
                new Object[] {
                        new String[] {"key11", "key11", "key21", "key21", "key22"},
                        new int[] {1, 2, 2, NULL_INT, 3},
                        new double[] {2.342, 0.0932, Double.NaN, NULL_DOUBLE, 3},
                        new DateTime[] {new DateTime(100), new DateTime(10000), null,
                                new DateTime(100000), new DateTime(1000000)}
                });;
        long numRows = tableToTest.size();

        String allSeparators = ",|\tzZ- €9@";
        for (char separator : allSeparators.toCharArray()) {
            String separatorStr = String.valueOf(separator);

            // Ignore separators in double quotes using this regex
            String splitterPattern = Pattern.quote(separatorStr) + "(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

            CsvTools.writeCsv(tableToTest, csvFile.getPath(), false, TimeZone.TZ_DEFAULT, false, separator, colNames);
            Scanner csvReader = new Scanner(csvFile);

            // Check header
            String header = csvReader.nextLine();
            String[] headerLine = header.split(splitterPattern);
            Assert.assertArrayEquals(colNames, headerLine);

            // Check rest of values
            for (int i = 0; i < numRows; i++) {
                Assert.assertTrue(csvReader.hasNextLine());
                String rawLine = csvReader.nextLine();
                String[] csvLine = rawLine.split(splitterPattern);
                Assert.assertEquals(numCols, csvLine.length);

                // Use separatorCsvEscape and compare the values
                for (int j = 0; j < numCols; j++) {
                    String valFromTable = tableToTest.getColumn(colNames[j]).get(i) == null
                            ? TableTools.nullToNullString(tableToTest.getColumn(colNames[j]).get(i))
                            : CsvTools.separatorCsvEscape(tableToTest.getColumn(colNames[j]).get(i).toString(),
                                    separatorStr);

                    Assert.assertEquals(valFromTable, csvLine[j]);
                }

            }

            // Check we exhausted the file
            Assert.assertFalse(csvReader.hasNextLine());
        }
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import io.deephaven.api.Selectable;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.util.TableTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class BrotliParquetTableReadWriteTest {

    private static final String ROOT_FILENAME = BrotliParquetTableReadWriteTest.class.getName() + "_root";

    private static File rootFile;

    @Before
    public void setUp() {
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        FileUtils.deleteRecursively(rootFile);
    }

    private static Table getTableFlat(int size, boolean includeSerializable) {
        QueryLibrary.importClass(SomeSillyTest.class);
        ArrayList<String> columns =
                new ArrayList<>(Arrays.asList("someStringColumn = i % 10 == 0?null:(`` + (i % 101))",
                        "nonNullString = `` + (i % 60)",
                        "nonNullPolyString = `` + (i % 600)",
                        "someIntColumn = i",
                        "someLongColumn = ii",
                        "someDoubleColumn = i*1.1",
                        "someFloatColumn = (float)(i*1.1)",
                        "someBoolColumn = i % 3 == 0?true:i%3 == 1?false:null",
                        "someShortColumn = (short)i",
                        "someByteColumn = (byte)i",
                        "someCharColumn = (char)i",
                        "someTime = DateTime.now() + i",
                        "someKey = `` + (int)(i /100)",
                        "nullKey = i < -1?`123`:null"));
        if (includeSerializable) {
            columns.add("someSerializable = new SomeSillyTest(i)");
        }
        return TableTools.emptyTable(size).select(
                Selectable.from(columns));
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

    private void compressionCodecTestHelper(final String codec) {
        final String currentCodec = ParquetInstructions.getDefaultCompressionCodecName();
        try {
            ParquetInstructions.setDefaultCompressionCodecName(codec);
            String path = rootFile + File.separator + "Table1.parquet";
            final Table table1 = getTableFlat(10000, false);
            ParquetTools.writeTable(table1, path);
            assertTrue(new File(path).length() > 0);
            final Table table2 = ParquetTools.readTable(path);
            TstUtils.assertTableEquals(table1, table2);
        } finally {
            ParquetInstructions.setDefaultCompressionCodecName(currentCodec);
        }
    }

    @Test
    public void testParquetBrotliCompressionCodec() {
        compressionCodecTestHelper("BROTLI");

    }
}

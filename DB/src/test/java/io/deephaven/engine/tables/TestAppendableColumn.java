/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.tables.utils.TableTools;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.tables.utils.ParquetTools;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class TestAppendableColumn extends TestCase {
    public void testOverflow() throws IOException {
        final File directory = Files.createTempDirectory("testOverflow").toFile();

        try {
            directory.mkdirs();

            Integer data[] = new Integer[80 * 4000];
            for (int i = 0; i < data.length; i++) {
                data[i] = i / 4;
            }

            final TableDefinition tableDefinition = TableDefinition.of(ColumnDefinition.ofInt("v").withGrouping());
            Table table = TableTools.newTable(tableDefinition, TableTools.col("v", data));
            File dest = new File(directory, "testOverflow.parquet");
            ParquetTools.writeTable(table, dest, tableDefinition);

            Table tableR = ParquetTools.readTable(dest);
            assertEquals(data.length, tableR.size());
            assertNotNull(tableR.getColumnSource("v").getGroupToRange());
            assertEquals(320000, tableR.getIndex().size());
            assertEquals(80000, tableR.getColumnSource("v").getGroupToRange().size());
            assertEquals(80000, tableR.getColumnSource("v").getValuesMapping(tableR.getIndex()).size());
            assertEquals(80000, tableR.getColumnSource("v")
                    .getValuesMapping(tableR.getIndex().subSetByPositionRange(0, tableR.size())).size());
            final Map mapper = tableR.getColumnSource("v").getGroupToRange();
            for (int i = 0; i < data.length / 4; i++) {
                assertEquals(mapper.get(i), RowSetFactoryImpl.INSTANCE.getRowSetByRange(i * 4, i * 4 + 3));
            }
        } finally {
            FileUtils.deleteRecursively(directory);
        }

    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import io.deephaven.base.FileUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.tables.utils.TableManagementTools;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class TestAppendableColumn extends TestCase {
    private static final TableManagementTools.StorageFormat storageFormat = TableManagementTools.StorageFormat.Parquet;
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
            TableManagementTools.writeTable(table, tableDefinition, directory, storageFormat);

            Table tableR = TableManagementTools.readTable(directory);
            assertEquals(data.length, tableR.size());
            assertNotNull(tableR.getColumnSource("v").getGroupToRange());
            assertEquals(320000, tableR.getIndex().size());
            assertEquals(80000, tableR.getColumnSource("v").getGroupToRange().size());
            assertEquals(80000, tableR.getColumnSource("v").getValuesMapping(tableR.getIndex()).size());
            assertEquals(80000, tableR.getColumnSource("v").getValuesMapping(tableR.getIndex().subindexByPos(0, tableR.size())).size());
            final Map mapper = tableR.getColumnSource("v").getGroupToRange();
            for (int i = 0; i < data.length / 4; i++) {
                assertEquals(mapper.get(i), Index.FACTORY.getIndexByRange(i * 4, i * 4 + 3));
            }
        }
        finally {
            FileUtils.deleteRecursively(directory);
        }

    }
}

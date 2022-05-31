/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.parquet.table.ParquetTools;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class TestParquetGrouping extends TestCase {

    public void testOverflow() throws IOException {
        // TODO: Figure out why this is called testOverflow
        final File directory = Files.createTempDirectory("testOverflow").toFile();

        try {
            directory.mkdirs();

            Integer data[] = new Integer[80_000 * 4];
            for (int i = 0; i < data.length; i++) {
                data[i] = i / 4;
            }

            final TableDefinition tableDefinition = TableDefinition.of(ColumnDefinition.ofInt("v").withGrouping());
            final Table table = TableTools.newTable(tableDefinition, TableTools.col("v", data));
            final ParquetInstructions instructions = ParquetInstructions.builder()
                    .addColumnNameMapping("V", "v")
                    .build();
            final File dest = new File(directory, "testOverflow.parquet");
            ParquetTools.writeTable(table, dest, tableDefinition, instructions);

            final Table tableR = ParquetTools.readTable(dest);
            assertEquals(data.length, tableR.size());
            assertNotNull(tableR.getColumnSource("V").getGroupToRange());
            assertEquals(80_000 * 4, tableR.getRowSet().size());
            assertEquals(80_000, tableR.getColumnSource("V").getGroupToRange().size());
            assertEquals(80_000, tableR.getColumnSource("V").getValuesMapping(tableR.getRowSet()).size());
            assertEquals(80_000, tableR.getColumnSource("V")
                    .getValuesMapping(tableR.getRowSet().subSetByPositionRange(0, tableR.size())).size());
            final Map mapper = tableR.getColumnSource("V").getGroupToRange();
            for (int i = 0; i < data.length / 4; i++) {
                assertEquals(mapper.get(i), RowSetFactory.fromRange(i * 4, i * 4 + 3));
            }
        } finally {
            FileUtils.deleteRecursively(directory);
        }

    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TestParquetIndexing extends RefreshingTableTestCase {

    public void testOverflow() throws IOException {
        // TODO: Figure out why this is called testOverflow
        final File directory = Files.createTempDirectory("testOverflow").toFile();

        try {
            directory.mkdirs();

            final int[] data = new int[80_000 * 4];
            for (int i = 0; i < data.length; i++) {
                data[i] = i / 4;
            }

            final Table table = TableTools.newTable(TableTools.intCol("v", data));
            DataIndexer.getOrCreateDataIndex(table, "v");

            final ParquetInstructions instructions = ParquetInstructions.builder()
                    .addColumnNameMapping("V", "v")
                    .build();
            final File dest = new File(directory, "testOverflow.parquet");
            ParquetTools.writeTable(table, dest.getPath(), instructions);

            final Table tableR = ParquetTools.readTable(dest.getPath());
            assertEquals(data.length, tableR.size());
            final DataIndex dataIndex = DataIndexer.getDataIndex(tableR, "V");
            Assert.neqNull(dataIndex, "dataIndex");
            final Table allGroupingTable = dataIndex.table();

            assertNotNull(allGroupingTable);
            assertEquals(80_000 * 4, tableR.getRowSet().size());
            assertEquals(80_000, allGroupingTable.size());

            final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup();

            final ColumnSource<RowSet> rowSetColumnSource = dataIndex.rowSetColumn();

            for (int i = 0; i < data.length / 4; i++) {
                final long key = rowKeyLookup.apply(i, false);
                assertEquals(rowSetColumnSource.get(key), RowSetFactory.fromRange(i * 4, i * 4 + 3));
            }

            // Clamp the index rowset and assert it is still correct.
            Table clampedTable = dataIndex.transform(DataIndexTransformer.builder()
                    .intersectRowSet(tableR.getRowSet())
                    .build()).table();
            assertEquals(80_000, clampedTable.size());

            clampedTable = dataIndex.transform(DataIndexTransformer.builder()
                    .intersectRowSet(tableR.getRowSet().subSetByPositionRange(0, tableR.size()))
                    .build()).table();
            assertEquals(80_000, clampedTable.size());
        } finally {
            FileUtils.deleteRecursively(directory);
        }
    }
}

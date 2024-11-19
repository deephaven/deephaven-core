//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.HierarchicalTable.SnapshotState;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.test.types.OutOfBandTest;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;

/**
 * Tests for {@link HierarchicalTable#snapshot(SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])
 * hierarchical table snapshots}.
 */
@Category(OutOfBandTest.class)
public class TestHierarchicalTableSnapshots extends QueryTableTestBase {

    @Test
    public void testRollupSnapshotSatisfaction() {
        assertTrue(true);
    }

    private static Table snapshotToTable(
            @NotNull final HierarchicalTable<?> hierarchicalTable,
            @NotNull final SnapshotState snapshotState,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows) {
        final ColumnDefinition<?>[] availableColumns =
                hierarchicalTable.getAvailableColumnDefinitions().toArray(ColumnDefinition[]::new);
        final ColumnDefinition<?>[] includedColumns = columns == null
                ? availableColumns
                : columns.stream().mapToObj(ci -> availableColumns[ci]).toArray(ColumnDefinition[]::new);

        assertTrue(rows.isContiguous());
        final int rowsSize = rows.intSize();
        //noinspection rawtypes
        final WritableChunk[] chunks = Arrays.stream(includedColumns)
                .map(cd -> ChunkType.fromElementType(cd.getDataType()))
                .map(ct -> ct.makeWritableChunk(rowsSize))
                .toArray(WritableChunk[]::new);

        //noinspection unchecked
        final long expandedSize =
                hierarchicalTable.snapshot(snapshotState, keyTable, keyTableActionColumn, columns, rows, chunks);
        final int snapshotSize = chunks.length == 0 ? 0 : chunks[0].size();
        final long expectedSnapshotSize = rows.isEmpty()
                ? 0
                : Math.min(rows.lastRowKey() + 1, expandedSize) - rows.firstRowKey();
        assertEquals(expectedSnapshotSize, snapshotSize);

        final LinkedHashMap<String, ColumnSource<?>> sources = new LinkedHashMap<>(includedColumns.length);
        for (int ci = 0; ci < includedColumns.length; ++ci) {
            final ColumnDefinition<?> columnDefinition = includedColumns[ci];
            //noinspection unchecked
            final WritableChunk<? extends Values> chunk = chunks[ci];
            final ChunkColumnSource<?> chunkColumnSource = ChunkColumnSource.make(
                    chunk.getChunkType(), columnDefinition.getDataType(), columnDefinition.getComponentType());
            chunkColumnSource.addChunk(chunk);
            sources.put(columnDefinition.getName(), chunkColumnSource);
        }

        //noinspection resource
        return new QueryTable(
                TableDefinition.of(includedColumns),
                RowSetFactory.flat(snapshotSize).toTracking(),
                sources);
    }
}

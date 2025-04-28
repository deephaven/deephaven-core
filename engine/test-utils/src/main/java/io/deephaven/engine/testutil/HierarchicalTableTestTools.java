//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ByteAsBooleanColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;

import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * When testing behavior of hierarchical tables, it is necessary to create snapshots for proper comparison. The
 * remainder of the test suite provides useful primitives for comparing tables. These functions perform a snapshot and
 * convert it to a Table.
 */
public class HierarchicalTableTestTools {
    /**
     * Generate a snapshot and convert it to a Table.
     *
     * <p>
     * You must release the result with {@link #freeSnapshotTableChunks(Table)} after use.
     * </p>
     *
     * @param hierarchicalTable the {@link HierarchicalTable} to snapshot
     * @param snapshotState the state created from the {@link HierarchicalTable#makeSnapshotState()} on
     *        hierarchicalTable
     * @param keyTable the keyTable for expansions
     * @param keyTableActionColumn the Action column within the key talbe
     * @param columns the columns to snapshot (null for all of htem)
     * @param rows the row positions to snapshot
     * @return a Table containing the snapshot. Must be released with {@link #freeSnapshotTableChunks(Table)} after use.
     */
    @SuppressWarnings("SameParameterValue")
    public static Table snapshotToTable(
            @NotNull final HierarchicalTable<?> hierarchicalTable,
            @NotNull final HierarchicalTable.SnapshotState snapshotState,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows) {
        final ColumnDefinition<?>[] availableColumns =
                hierarchicalTable.getAvailableColumnDefinitions().toArray(ColumnDefinition[]::new);
        final ColumnDefinition<?>[] includedColumns = columns == null
                ? availableColumns
                : columns.stream().mapToObj(ci -> availableColumns[ci]).toArray(ColumnDefinition[]::new);

        assertThat(rows.isContiguous()).isTrue();
        final int rowsSize = rows.intSize();
        // noinspection rawtypes
        final WritableChunk[] chunks = Arrays.stream(includedColumns)
                .map(cd -> maybeConvertToPrimitiveChunkType(cd.getDataType()))
                .map(ct -> ct.makeWritableChunk(rowsSize))
                .toArray(WritableChunk[]::new);

        // noinspection unchecked
        final long expandedSize =
                hierarchicalTable.snapshot(snapshotState, keyTable, keyTableActionColumn, columns, rows, chunks);
        final int snapshotSize = chunks.length == 0 ? 0 : chunks[0].size();
        final long expectedSnapshotSize = rows.isEmpty()
                ? 0
                : Math.min(rows.lastRowKey() + 1, expandedSize) - rows.firstRowKey();
        assertThat(snapshotSize).isEqualTo(expectedSnapshotSize);

        final LinkedHashMap<String, ColumnDefinition<?>> dedupedColumnMap = new LinkedHashMap<>();
        for (final ColumnDefinition<?> columnDefinition : includedColumns) {
            String origName = columnDefinition.getName();
            String name = origName;
            for (int idx = 0; dedupedColumnMap.containsKey(name); idx++) {
                name = origName + idx++;
            }
            dedupedColumnMap.put(name, columnDefinition.withName(name));
        }
        final ColumnDefinition<?>[] dedupedColumns = dedupedColumnMap.values().toArray(ColumnDefinition[]::new);

        final LinkedHashMap<String, ColumnSource<?>> sources = new LinkedHashMap<>();
        for (int ci = 0; ci < dedupedColumns.length; ++ci) {
            final ColumnDefinition<?> columnDefinition = dedupedColumns[ci];
            // noinspection unchecked
            final WritableChunk<? extends Values> chunk = chunks[ci];
            final ChunkColumnSource<?> chunkColumnSource = ChunkColumnSource.make(
                    chunk.getChunkType(), columnDefinition.getDataType(), columnDefinition.getComponentType());
            if (snapshotSize > 0) {
                chunkColumnSource.addChunk(chunk);
            }
            final ColumnSource<?> source;
            if (columnDefinition.getDataType() == Boolean.class && chunkColumnSource.getType() == byte.class) {
                // noinspection unchecked
                source = byteToBooleanSource((ColumnSource<Byte>) chunkColumnSource);
            } else if (columnDefinition.getDataType() == Instant.class && chunkColumnSource.getType() == long.class) {
                // noinspection unchecked
                source = longToInstantSource((ColumnSource<Long>) chunkColumnSource);
            } else {
                source = chunkColumnSource;
            }
            sources.put(columnDefinition.getName(), source);
        }

        // noinspection resource
        return new QueryTable(
                TableDefinition.of(dedupedColumns),
                RowSetFactory.flat(snapshotSize).toTracking(),
                sources);
    }

    public static void freeSnapshotTableChunks(@NotNull final Table snapshotTable) {
        snapshotTable.getColumnSources().forEach(cs -> {
            if (cs instanceof ByteAsBooleanColumnSource) {
                ((ChunkColumnSource<?>) cs.reinterpret(byte.class)).clear();
            } else if (cs instanceof LongAsInstantColumnSource) {
                ((ChunkColumnSource<?>) cs.reinterpret(long.class)).clear();
            } else {
                ((ChunkColumnSource<?>) cs).clear();
            }
        });
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.SymbolTableToUniqueIdSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;

import java.util.Arrays;

public class StaticSymbolTableChunkedOperatorAggregationStateManager implements OperatorAggregationStateManager {
    private static final int CHUNK_SIZE = ChunkedOperatorAggregationHelper.CHUNK_SIZE;

    private final Table symbolTable;
    private final int tableSize;

    private final SymbolTableToUniqueIdSource mappedKeySource;
    private final ObjectArraySource<String> keyColumn = new ObjectArraySource<>(String.class);

    private int nullPosition = -1;
    private final int[] keyPositions;
    private int nextPosition = 0;

    StaticSymbolTableChunkedOperatorAggregationStateManager(final ColumnSource<?> keySource, final Table symbolTable) {
        this.symbolTable = symbolTable;
        tableSize = symbolTable.intSize();

        mappedKeySource = SymbolTableToUniqueIdSource.getUniqueIdSource(symbolTable, keySource);

        keyPositions = new int[tableSize];
        Arrays.fill(keyPositions, -1);
    }

    @Override
    public int maxTableSize() {
        return tableSize;
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(final ColumnSource<?>[] buildSources, long maxSize) {
        return null;
    }

    @Override
    public void add(final SafeCloseable bc, final RowSequence rowSequence, final ColumnSource<?>[] sources,
            final MutableInt nextOutputPosition, final WritableIntChunk<RowKeys> outputPositions) {
        if (rowSequence.isEmpty()) {
            return;
        }

        outputPositions.setSize(rowSequence.intSize());

        final int maxChunkSize = Math.min(rowSequence.intSize(), CHUNK_SIZE);

        try (final RowSequence.Iterator rsIt = rowSequence.getRowSequenceIterator();
                final ChunkSource.FillContext fillContext = mappedKeySource.makeFillContext(maxChunkSize);
                final WritableLongChunk<RowKeys> symbolTableValues = WritableLongChunk.makeWritableChunk(tableSize);
                final WritableIntChunk<Values> symbolLookupChunk = WritableIntChunk.makeWritableChunk(maxChunkSize)) {

            symbolTableValues.setSize(0);

            final int firstNewPosition = nextPosition;

            while (rsIt.hasMore()) {
                final RowSequence nextKeys = rsIt.getNextRowSequenceWithLength(maxChunkSize);
                final LongChunk<Values> symbolSourceChunk =
                        mappedKeySource.fillChunkWithSymbolSource(fillContext, symbolLookupChunk, nextKeys);

                final int chunkSize = symbolLookupChunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final int key = symbolLookupChunk.get(ii);
                    if (key == QueryConstants.NULL_INT) {
                        if (nullPosition == -1) {
                            nullPosition = nextPosition++;
                        }
                        outputPositions.set(ii, nullPosition);
                    } else {
                        final int keyPosition = keyPositions[key];
                        if (keyPosition == -1) {
                            outputPositions.set(ii, keyPositions[key] = nextPosition++);
                            symbolTableValues.add(symbolSourceChunk.get(ii));
                        } else {
                            outputPositions.set(ii, keyPosition);
                        }
                    }
                }
            }

            if (nextPosition != firstNewPosition) {
                updateKeyHashTableSources(symbolTableValues, firstNewPosition);
            }
        }

        nextOutputPosition.set(nextPosition);
    }

    private void updateKeyHashTableSources(final WritableLongChunk<RowKeys> symbolTableValues,
            final int firstNewPosition) {
        keyColumn.ensureCapacity(nextPosition);

        final ColumnSource<?> symbolColumnSource = symbolTable.getColumnSource(SymbolTableSource.SYMBOL_COLUMN_NAME);

        int symbolIterIdx = 0;
        int localPosition = firstNewPosition;
        if (nullPosition >= firstNewPosition) {
            while (symbolIterIdx < nullPosition - firstNewPosition) {
                keyColumn.set(localPosition++, (String) symbolColumnSource.get(symbolTableValues.get(symbolIterIdx++)));
            }

            keyColumn.set(localPosition++, null);
        }

        while (symbolIterIdx < symbolTableValues.size()) {
            keyColumn.set(localPosition++, (String) symbolColumnSource.get(symbolTableValues.get(symbolIterIdx++)));
        }
    }

    @Override
    public ColumnSource<?>[] getKeyHashTableSources() {
        return new ColumnSource[] {keyColumn};
    }

    @Override
    public int findPositionForKey(final Object key) {
        // shouldn't be able to get here; rollup/treeview will call this when we're 2+ levels deep in out view. since
        // we're limited to a single keySource, we cannot be more than 1 level deep
        throw new UnsupportedOperationException("StaticSymbolTable StateManager must be used with a single keySource");
    }
}

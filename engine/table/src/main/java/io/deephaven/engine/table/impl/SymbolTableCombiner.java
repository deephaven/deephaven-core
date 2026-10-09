//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.MathUtil;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.join.KeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;

/**
 * Gives each distinct symbol in one or more symbol tables a dense unique identifier, and maps the symbol table ids to
 * those identifiers.
 */
class SymbolTableCombiner {
    private static final int MINIMUM_INITIAL_HASH_SIZE = CHUNK_SIZE;
    private static final double MAXIMUM_LOAD_FACTOR = 0.75;

    private final KeyIdHasherTypedBase hasher;

    /**
     * @param tableKeySources a single source with the symbol type
     * @param tableSize the initial hash table size, a power of two
     */
    SymbolTableCombiner(ColumnSource<?>[] tableKeySources, int tableSize) {
        hasher = KeyIdHasherTypedBase.make(tableKeySources, tableSize, MAXIMUM_LOAD_FACTOR);
    }

    void addSymbols(final Table symbolTable, IntegerSparseArraySource symbolMapper) {
        if (symbolTable.isEmpty()) {
            return;
        }
        addSymbols(symbolTable, symbolTable.getRowSet(), symbolMapper);
    }

    void addSymbols(final Table symbolTable, RowSet rowSet, IntegerSparseArraySource symbolMapper) {
        if (symbolTable.isEmpty()) {
            return;
        }
        final ColumnSource<?>[] symbolSources = {symbolTable.getColumnSource(SymbolTableSource.SYMBOL_COLUMN_NAME)};
        final ColumnSource<Long> idSource = symbolTable.getColumnSource(SymbolTableSource.ID_COLUMN_NAME);
        try (final ColumnSource.GetContext idContext =
                idSource.makeGetContext((int) Math.min(CHUNK_SIZE, rowSet.size()))) {
            hasher.build(rowSet, symbolSources,
                    (rows, uniqueIds) -> mapSymbols(idSource, idContext, rows, uniqueIds, symbolMapper, 0));
        }
    }

    /**
     * Map the ids of {@code symbolTable} to the unique identifiers of their symbols, without adding symbols.
     *
     * @param symbolTable the symbol table
     * @param symbolMapper receives the unique identifier of each symbol table id
     * @param irrelevantSymbolValue the unique identifier for the ids of symbols that have not been added
     */
    void lookupSymbols(final Table symbolTable, IntegerSparseArraySource symbolMapper,
            @SuppressWarnings("SameParameterValue") int irrelevantSymbolValue) {
        if (symbolTable.isEmpty()) {
            return;
        }
        final ColumnSource<?>[] symbolSources = {symbolTable.getColumnSource(SymbolTableSource.SYMBOL_COLUMN_NAME)};
        final ColumnSource<Long> idSource = symbolTable.getColumnSource(SymbolTableSource.ID_COLUMN_NAME);
        try (final ColumnSource.GetContext idContext =
                idSource.makeGetContext((int) Math.min(CHUNK_SIZE, symbolTable.size()))) {
            hasher.probe(symbolTable.getRowSet(), symbolSources, false,
                    (rows, uniqueIds) -> mapSymbols(idSource, idContext, rows, uniqueIds, symbolMapper,
                            irrelevantSymbolValue));
        }
    }

    private static void mapSymbols(
            final ColumnSource<Long> idSource,
            final ColumnSource.GetContext idContext,
            final RowSequence rows,
            final IntChunk<Values> uniqueIds,
            final IntegerSparseArraySource symbolMapper,
            final int irrelevantSymbolValue) {
        final LongChunk<? extends Values> ids = idSource.getChunk(idContext, rows).asLongChunk();
        for (int ii = 0; ii < ids.size(); ++ii) {
            final int uniqueId = uniqueIds.get(ii);
            symbolMapper.set(ids.get(ii),
                    uniqueId == KeyIdHasherTypedBase.NULL_ID ? irrelevantSymbolValue : uniqueId);
        }
    }

    int getMaximumIdentifier() {
        return hasher.idCapacity();
    }

    /**
     * @param initialCapacity the number of symbols the combiner will hold
     * @return a hash table size that holds {@code initialCapacity} symbols under the load factor, so that adding them
     *         does not rehash
     */
    static int hashTableSize(long initialCapacity) {
        final long minimumSize = (long) Math.ceil(initialCapacity / MAXIMUM_LOAD_FACTOR);
        return (int) Math.max(MINIMUM_INITIAL_HASH_SIZE,
                Math.min(MAX_TABLE_SIZE, MathUtil.roundUpPowerOf2(minimumSize)));
    }
}

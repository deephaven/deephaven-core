//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import org.jetbrains.annotations.NotNull;

/**
 * This column source is used as a wrapper for the original table's symbol sources.
 * <p>
 * The symbol sources are reinterpreted to longs, and then the SymbolCombiner produces an IntegerSparseArraySource for
 * each side. To convert from the symbol table value, we simply look it up in the symbolLookup source and use that as
 * our chunked result.
 */
public class SymbolTableToUniqueIdSource extends AbstractColumnSource<Integer>
        implements ImmutableColumnSourceGetDefaults.ForInt {
    private final ColumnSource<Long> symbolSource;
    private final IntegerSparseArraySource symbolLookup;

    SymbolTableToUniqueIdSource(ColumnSource<Long> symbolSource, IntegerSparseArraySource symbolLookup) {
        super(int.class);
        this.symbolSource = symbolSource;
        this.symbolLookup = symbolLookup;
    }

    @Override
    public int getInt(long rowKey) {
        final long symbolId = symbolSource.getLong(rowKey);
        return symbolLookup.getInt(symbolId);
    }

    private class LongToIntFillContext implements FillContext {
        final WritableLongChunk<Values> longChunk;
        final FillContext innerFillContext;

        LongToIntFillContext(final int chunkCapacity, final SharedContext sharedState) {
            longChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
            innerFillContext = symbolSource.makeFillContext(chunkCapacity, sharedState);
        }

        @Override
        public void close() {
            longChunk.close();
            innerFillContext.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new LongToIntFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence orderedKeys) {
        fillChunkWithSymbolSource(context, destination, orderedKeys);
    }

    public WritableLongChunk<Values> fillChunkWithSymbolSource(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence orderedKeys) {
        final WritableIntChunk<? super Values> destAsInt = destination.asWritableIntChunk();
        final LongToIntFillContext longToIntContext = (LongToIntFillContext) context;
        final WritableLongChunk<Values> longChunk = longToIntContext.longChunk;
        symbolSource.fillChunk(longToIntContext.innerFillContext, longChunk, orderedKeys);
        for (int ii = 0; ii < longChunk.size(); ++ii) {
            destAsInt.set(ii, symbolLookup.getInt(longChunk.get(ii)));
        }
        destination.setSize(longChunk.size());

        return longChunk;
    }

    public static SymbolTableToUniqueIdSource getUniqueIdSource(final Table symbolTable,
            final ColumnSource<?> keySource) {
        final IntegerSparseArraySource symbolMapper = new IntegerSparseArraySource();
        if (symbolTable.size() > 0) {
            final SymbolTableCombiner stc = new SymbolTableCombiner(new ColumnSource<?>[] {keySource},
                    Integer.highestOneBit(symbolTable.intSize()) << 1);
            stc.addSymbols(symbolTable, symbolMapper);
        }

        return new SymbolTableToUniqueIdSource(keySource.reinterpret(long.class), symbolMapper);
    }

    @Override
    public boolean isStateless() {
        return symbolSource.isStateless();
    }
}

package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;

public interface FormulaKernel {
    Formula.FillContext makeFillContext(final int __chunkCapacity);

    void applyFormulaChunk(Formula.FillContext __context, final WritableChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);
}

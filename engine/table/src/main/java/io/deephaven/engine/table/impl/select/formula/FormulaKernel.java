package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.WritableChunk;

public interface FormulaKernel {
    Formula.FillContext makeFillContext(final int __chunkCapacity);

    void applyFormulaChunk(Formula.FillContext __context, final WritableChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);
}

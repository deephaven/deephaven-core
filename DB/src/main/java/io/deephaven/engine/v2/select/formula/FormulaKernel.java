package io.deephaven.engine.v2.select.formula;

import io.deephaven.engine.v2.select.Formula;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.Chunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;

public interface FormulaKernel {
    Formula.FillContext makeFillContext(final int __chunkCapacity);

    void applyFormulaChunk(Formula.FillContext __context, final WritableChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);
}

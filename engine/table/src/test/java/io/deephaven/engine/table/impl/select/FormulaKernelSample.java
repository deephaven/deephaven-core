package io.deephaven.engine.table.impl.select;
// QueryLibrary internal version number: DEFAULT
import io.deephaven.chunk.attributes.*;

import java.lang.*;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;

public class FormulaKernelSample implements io.deephaven.engine.table.impl.select.formula.FormulaKernel {
    public static final io.deephaven.engine.table.impl.select.formula.FormulaKernelFactory __FORMULA_KERNEL_FACTORY = FormulaKernelSample::new;

    private final io.deephaven.vector.LongVector II_;
    private final java.lang.Integer q;

    public FormulaKernelSample(io.deephaven.vector.Vector[] __vectors,
            io.deephaven.engine.context.QueryScopeParam[] __params) {
        II_ = (io.deephaven.vector.LongVector)__vectors[0];
        q = (java.lang.Integer)__params[0].getValue();
    }

    @Override
    public FormulaFillContext makeFillContext(final int __chunkCapacity) {
        return new FormulaFillContext(__chunkCapacity);
    }

    @Override
    public void applyFormulaChunk(io.deephaven.engine.table.impl.select.Formula.FillContext __context,
            final WritableChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final WritableLongChunk<? super Values> __typedDestination = __destination.asWritableLongChunk();
        final LongChunk<? extends Values> __chunk__col__II = __sources[0].asLongChunk();
        final LongChunk<? extends Values> __chunk__col__ii = __sources[1].asLongChunk();
        final IntChunk<? extends Values> __chunk__col__I = __sources[2].asIntChunk();
        final IntChunk<? extends Values> __chunk__col__i = __sources[3].asIntChunk();
        final int __size = __typedDestination.size();
        for (int __chunkPos = 0; __chunkPos < __size; ++__chunkPos) {
            __typedDestination.set(__chunkPos, applyFormulaPerItem(__chunk__col__II.get(__chunkPos), __chunk__col__ii.get(__chunkPos), __chunk__col__I.get(__chunkPos), __chunk__col__i.get(__chunkPos)));
        }
    }

    private long applyFormulaPerItem(long II, long ii, int I, int i) {
        try {
            return plus(plus(multiply(I, II), multiply(q.intValue(), ii)), II_.get(minus(i, 1)));
        } catch (java.lang.Exception __e) {
            throw new io.deephaven.engine.table.impl.select.FormulaEvaluationException("In formula: " + "plus(plus(multiply(I, II), multiply(q.intValue(), ii)), II_.get(minus(i, 1)))", __e);
        }
    }

    private class FormulaFillContext implements io.deephaven.engine.table.impl.select.Formula.FillContext {
        FormulaFillContext(int __chunkCapacity) {
        }
    }

}
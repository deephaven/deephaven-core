package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.impl.select.Formula.FillContext;
import io.deephaven.engine.table.impl.select.formula.FormulaKernel;
import io.deephaven.engine.chunk.Attributes.Any;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.chunk.BooleanChunk;
import io.deephaven.engine.chunk.ByteChunk;
import io.deephaven.engine.chunk.CharChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.Chunk.Visitor;
import io.deephaven.engine.chunk.DoubleChunk;
import io.deephaven.engine.chunk.FloatChunk;
import io.deephaven.engine.chunk.IntChunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.ShortChunk;
import io.deephaven.engine.chunk.WritableBooleanChunk;
import io.deephaven.engine.chunk.WritableByteChunk;
import io.deephaven.engine.chunk.WritableCharChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableDoubleChunk;
import io.deephaven.engine.chunk.WritableFloatChunk;
import io.deephaven.engine.chunk.WritableIntChunk;
import io.deephaven.engine.chunk.WritableLongChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.chunk.WritableShortChunk;

/**
 * Extends {@link FormulaKernel} for specifically typed destination {@link WritableChunk WritableChunks}.
 */
public abstract class FormulaKernelTypedBase implements FormulaKernel {

    @Override
    public final void applyFormulaChunk(
            FillContext __context,
            WritableChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        __destination.walk(new ToTypedMethod<>(__context, __sources));
    }

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableByteChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableBooleanChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableCharChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableShortChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableIntChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableLongChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableFloatChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract void applyFormulaChunk(
            Formula.FillContext __context,
            WritableDoubleChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    public abstract <T> void applyFormulaChunk(
            Formula.FillContext __context,
            WritableObjectChunk<T, ? super Values> __destination,
            Chunk<? extends Values>[] __sources);

    private class ToTypedMethod<ATTR extends Any> implements Visitor<ATTR> {
        private final FillContext __context;
        private final Chunk<? extends Values>[] __sources;

        ToTypedMethod(FillContext __context, Chunk<? extends Values>[] __sources) {
            this.__context = __context;
            this.__sources = __sources;
        }

        @Override
        public void visit(ByteChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableByteChunk(),
                    __sources);
        }

        @Override
        public void visit(BooleanChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableBooleanChunk(),
                    __sources);
        }

        @Override
        public void visit(CharChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableCharChunk(),
                    __sources);
        }

        @Override
        public void visit(ShortChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableShortChunk(),
                    __sources);
        }

        @Override
        public void visit(IntChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableIntChunk(),
                    __sources);
        }

        @Override
        public void visit(LongChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableLongChunk(),
                    __sources);
        }

        @Override
        public void visit(FloatChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableFloatChunk(),
                    __sources);
        }

        @Override
        public void visit(DoubleChunk<ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableDoubleChunk(),
                    __sources);
        }

        @Override
        public <T> void visit(ObjectChunk<T, ATTR> chunk) {
            // noinspection unchecked,rawtypes
            applyFormulaChunk(
                    __context,
                    ((WritableChunk) chunk).asWritableObjectChunk(),
                    __sources);
        }
    }
}

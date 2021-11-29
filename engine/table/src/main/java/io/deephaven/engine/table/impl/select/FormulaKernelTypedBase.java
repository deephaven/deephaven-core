package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.impl.select.Formula.FillContext;
import io.deephaven.engine.table.impl.select.formula.FormulaKernel;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.BooleanChunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.Chunk.Visitor;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;

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

package io.deephaven.db.v2.select;

import io.deephaven.db.v2.select.Formula.FillContext;
import io.deephaven.db.v2.select.formula.FormulaKernel;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.BooleanChunk;
import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.CharChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.Chunk.Visitor;
import io.deephaven.db.v2.sources.chunk.DoubleChunk;
import io.deephaven.db.v2.sources.chunk.FloatChunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.ShortChunk;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;
import io.deephaven.db.v2.sources.chunk.WritableByteChunk;
import io.deephaven.db.v2.sources.chunk.WritableCharChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableDoubleChunk;
import io.deephaven.db.v2.sources.chunk.WritableFloatChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableShortChunk;

/**
 * Extends {@link FormulaKernel} for specifically typed destination {@link WritableChunk
 * WritableChunks}.
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

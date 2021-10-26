package io.deephaven.engine.v2.select;
// QueryLibrary internal version number: DEFAULT
import java.lang.*;
import java.util.*;

import io.deephaven.engine.util.LongSizedDataStructure;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.IntChunk;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
import io.deephaven.engine.v2.sources.chunk.WritableIntChunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.TrackingRowSet;
import io.deephaven.util.type.TypeUtils;

import java.util.concurrent.ConcurrentHashMap;

import static io.deephaven.engine.tables.lang.DBLanguageFunctionUtil.*;

public class FormulaSample extends io.deephaven.engine.v2.select.Formula {
    public static final io.deephaven.engine.v2.select.formula.FormulaFactory __FORMULA_FACTORY = FormulaSample::new;

    private final io.deephaven.engine.v2.sources.ColumnSource<java.lang.Long> II;
    private final io.deephaven.engine.v2.sources.ColumnSource<java.lang.Integer> I;
    private final io.deephaven.engine.tables.dbarrays.DbLongArray II_;
    private final java.lang.Integer q;
    private final Map<Object, Object> __lazyResultCache;


    public FormulaSample(final TrackingRowSet rowSet,
            final boolean __lazy,
            final java.util.Map<String, ? extends io.deephaven.engine.v2.sources.ColumnSource> __columnsToData,
            final io.deephaven.engine.tables.select.Param... __params) {
        super(rowSet);
        II = __columnsToData.get("II");
        I = __columnsToData.get("I");
        II_ = new io.deephaven.engine.v2.dbarrays.DbLongArrayColumnWrapper(__columnsToData.get("II"), __rowSet);
        q = (java.lang.Integer) __params[0].getValue();
        __lazyResultCache = __lazy ? new ConcurrentHashMap<>() : null;
    }

    @Override
    public long getLong(final long k) {
        final long findResult = __rowSet.find(k);
        final int i = __intSize(findResult);
        final long ii = findResult;
        final long __temp0 = II.getLong(k);
        final int __temp1 = I.getInt(k);
        if (__lazyResultCache != null) {
            final Object __lazyKey = io.deephaven.engine.util.caching.C14nUtil.maybeMakeSmartKey(i, ii, __temp0, __temp1);
            return (long)__lazyResultCache.computeIfAbsent(__lazyKey, __unusedKey -> applyFormulaPerItem(i, ii, __temp0, __temp1));
        }
        return applyFormulaPerItem(i, ii, __temp0, __temp1);
    }

    @Override
    public long getPrevLong(final long k) {
        final long findResult;
        try (final RowSet prev = __rowSet.getPrevRowSet()) {
            findResult = prev.find(k);
        }
        final int i = __intSize(findResult);
        final long ii = findResult;
        final long __temp0 = II.getPrevLong(k);
        final int __temp1 = I.getPrevInt(k);
        if (__lazyResultCache != null) {
            final Object __lazyKey = io.deephaven.engine.util.caching.C14nUtil.maybeMakeSmartKey(i, ii, __temp0, __temp1);
            return (long)__lazyResultCache.computeIfAbsent(__lazyKey, __unusedKey -> applyFormulaPerItem(i, ii, __temp0, __temp1));
        }
        return applyFormulaPerItem(i, ii, __temp0, __temp1);
    }

    @Override
    public Object get(final long k) {
        return TypeUtils.box(getLong(k));
    }

    @Override
    public Object getPrev(final long k) {
        return TypeUtils.box(getPrevLong(k));
    }

    @Override
    protected io.deephaven.engine.v2.sources.chunk.ChunkType getChunkType() {
        return io.deephaven.engine.v2.sources.chunk.ChunkType.Long;
    }

    @Override
    public void fillChunk(final FillContext __context, final WritableChunk<? super Attributes.Values> __destination, final RowSequence __rowSequence) {
        final FormulaFillContext __typedContext = (FormulaFillContext)__context;
        final LongChunk<? extends Attributes.Values> __chunk__col__II = this.II.getChunk(__typedContext.__subContextII, __rowSequence).asLongChunk();
        final IntChunk<? extends Attributes.Values> __chunk__col__I = this.I.getChunk(__typedContext.__subContextI, __rowSequence).asIntChunk();
        fillChunkHelper(false, __typedContext, __destination, __rowSequence, __chunk__col__II, __chunk__col__I);
    }

    @Override
    public void fillPrevChunk(final FillContext __context, final WritableChunk<? super Attributes.Values> __destination, final RowSequence __rowSequence) {
        final FormulaFillContext __typedContext = (FormulaFillContext)__context;
        final LongChunk<? extends Attributes.Values> __chunk__col__II = this.II.getPrevChunk(__typedContext.__subContextII, __rowSequence).asLongChunk();
        final IntChunk<? extends Attributes.Values> __chunk__col__I = this.I.getPrevChunk(__typedContext.__subContextI, __rowSequence).asIntChunk();
        fillChunkHelper(true, __typedContext, __destination, __rowSequence, __chunk__col__II, __chunk__col__I);
    }

    private void fillChunkHelper(final boolean __usePrev, final FormulaFillContext __context,
                                 final WritableChunk<? super Attributes.Values> __destination,
                                 final RowSequence __rowSequence, LongChunk<? extends Attributes.Values> __chunk__col__II, IntChunk<? extends Attributes.Values> __chunk__col__I) {
        final WritableLongChunk<? super Attributes.Values> __typedDestination = __destination.asWritableLongChunk();
        try (final RowSet prev = __usePrev ? __rowSet.getPrevRowSet() : null;
             final RowSet inverted = ((prev != null) ? prev : __rowSet).invert(__rowSequence.asRowSet())) {
            __context.__iChunk.setSize(0);
            inverted.forAllLongs(l -> __context.__iChunk.add(__intSize(l)));
            inverted.fillRowKeyChunk(__context.__iiChunk);
        }
        final int[] __chunkPosHolder = new int[] {0};
        if (__lazyResultCache != null) {
            __rowSequence.forAllLongs(k -> {
                final int __chunkPos = __chunkPosHolder[0]++;
                final int i = __context.__iChunk.get(__chunkPos);
                final long ii = __context.__iiChunk.get(__chunkPos);
                final Object __lazyKey = io.deephaven.engine.util.caching.C14nUtil.maybeMakeSmartKey(i, ii, __chunk__col__II.get(__chunkPos), __chunk__col__I.get(__chunkPos));
                __typedDestination.set(__chunkPos, (long)__lazyResultCache.computeIfAbsent(__lazyKey, __unusedKey -> applyFormulaPerItem(i, ii, __chunk__col__II.get(__chunkPos), __chunk__col__I.get(__chunkPos))));
            }
            );
        } else {
            __rowSequence.forAllLongs(k -> {
                final int __chunkPos = __chunkPosHolder[0]++;
                final int i = __context.__iChunk.get(__chunkPos);
                final long ii = __context.__iiChunk.get(__chunkPos);
                __typedDestination.set(__chunkPos, applyFormulaPerItem(i, ii, __chunk__col__II.get(__chunkPos), __chunk__col__I.get(__chunkPos)));
            }
            );
        }
        __typedDestination.setSize(__chunkPosHolder[0]);
    }

    private long applyFormulaPerItem(int i, long ii, long II, int I) {
        try {
            return plus(plus(multiply(I, II), multiply(q.intValue(), ii)), II_.get(minus(i, 1)));
        } catch (java.lang.Exception __e) {
            throw new io.deephaven.engine.v2.select.FormulaEvaluationException("In formula: Value = " + "plus(plus(multiply(I, II), multiply(q.intValue(), ii)), II_.get(minus(i, 1)))", __e);
        }
    }

    @Override
    public FormulaFillContext makeFillContext(final int __chunkCapacity) {
        return new FormulaFillContext(__chunkCapacity);
    }

    private class FormulaFillContext implements io.deephaven.engine.v2.select.Formula.FillContext {
        private final WritableIntChunk<Attributes.OrderedRowKeys> __iChunk;
        private final WritableLongChunk<Attributes.OrderedRowKeys> __iiChunk;
        private final ColumnSource.GetContext __subContextII;
        private final ColumnSource.GetContext __subContextI;
        FormulaFillContext(int __chunkCapacity) {
            __iChunk = WritableIntChunk.makeWritableChunk(__chunkCapacity);
            __iiChunk = WritableLongChunk.makeWritableChunk(__chunkCapacity);
            __subContextII = II.makeGetContext(__chunkCapacity);
            __subContextI = I.makeGetContext(__chunkCapacity);
        }

        @Override
        public void close() {
            __iChunk.close();
            __iiChunk.close();
            __subContextII.close();
            __subContextI.close();
        }
    }

    private int __intSize(final long l) {
        return LongSizedDataStructure.intSize("FormulaColumn ii usage", l);
    }
}

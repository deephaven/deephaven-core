package io.deephaven.engine.table.impl.select;
// QueryLibrary internal version number: DEFAULT
import io.deephaven.chunk.attributes.*;
import io.deephaven.engine.rowset.chunkattributes.*;
import java.lang.*;
import java.util.*;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import static io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.Period;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.VectorConversions;
import java.lang.reflect.Array;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.LocalTime;
import static io.deephaven.base.string.cache.CompressedString.*;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;
import static io.deephaven.engine.table.impl.verify.TableAssertions.*;
import static io.deephaven.engine.util.ColorUtilImpl.*;
import static io.deephaven.function.BinSearch.*;
import static io.deephaven.function.BooleanPrimitives.*;
import static io.deephaven.function.ByteNumericPrimitives.*;
import static io.deephaven.function.BytePrimitives.*;
import static io.deephaven.function.Casting.*;
import static io.deephaven.function.CharacterPrimitives.*;
import static io.deephaven.function.ComparePrimitives.*;
import static io.deephaven.function.DoubleFpPrimitives.*;
import static io.deephaven.function.DoubleNumericPrimitives.*;
import static io.deephaven.function.DoublePrimitives.*;
import static io.deephaven.function.FloatFpPrimitives.*;
import static io.deephaven.function.FloatNumericPrimitives.*;
import static io.deephaven.function.FloatPrimitives.*;
import static io.deephaven.function.IntegerNumericPrimitives.*;
import static io.deephaven.function.IntegerPrimitives.*;
import static io.deephaven.function.LongNumericPrimitives.*;
import static io.deephaven.function.LongPrimitives.*;
import static io.deephaven.function.ObjectPrimitives.*;
import static io.deephaven.function.PrimitiveParseUtil.*;
import static io.deephaven.function.ShortNumericPrimitives.*;
import static io.deephaven.function.ShortPrimitives.*;
import static io.deephaven.gui.color.Color.*;
import static io.deephaven.time.DateTimeUtils.*;
import static io.deephaven.time.TimeZone.*;
import static io.deephaven.time.calendar.StaticCalendarMethods.*;
import static io.deephaven.util.QueryConstants.*;

public class FormulaSample extends io.deephaven.engine.table.impl.select.Formula {
    public static final io.deephaven.engine.table.impl.select.formula.FormulaFactory __FORMULA_FACTORY = FormulaSample::new;

    private final io.deephaven.engine.table.ColumnSource<java.lang.Long> II;
    private final io.deephaven.engine.table.ColumnSource<java.lang.Integer> I;
    private final io.deephaven.vector.LongVector II_;
    private final java.lang.Integer q;
    private final Map<Object, Object> __lazyResultCache;


    public FormulaSample(final TrackingRowSet rowSet,
            final boolean __lazy,
            final java.util.Map<String, ? extends io.deephaven.engine.table.ColumnSource> __columnsToData,
            final io.deephaven.engine.table.lang.QueryScopeParam... __params) {
        super(rowSet);
        II = __columnsToData.get("II");
        I = __columnsToData.get("I");
        II_ = new io.deephaven.engine.table.impl.vector.LongVectorColumnWrapper(__columnsToData.get("II"), __rowSet);
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
        try (final RowSet prev = __rowSet.copyPrev()) {
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
    protected io.deephaven.chunk.ChunkType getChunkType() {
        return io.deephaven.chunk.ChunkType.Long;
    }

    @Override
    public void fillChunk(final FillContext __context, final WritableChunk<? super Values> __destination, final RowSequence __rowSequence) {
        final FormulaFillContext __typedContext = (FormulaFillContext)__context;
        final LongChunk<? extends Values> __chunk__col__II = this.II.getChunk(__typedContext.__subContextII, __rowSequence).asLongChunk();
        final IntChunk<? extends Values> __chunk__col__I = this.I.getChunk(__typedContext.__subContextI, __rowSequence).asIntChunk();
        fillChunkHelper(false, __typedContext, __destination, __rowSequence, __chunk__col__II, __chunk__col__I);
    }

    @Override
    public void fillPrevChunk(final FillContext __context, final WritableChunk<? super Values> __destination, final RowSequence __rowSequence) {
        final FormulaFillContext __typedContext = (FormulaFillContext)__context;
        final LongChunk<? extends Values> __chunk__col__II = this.II.getPrevChunk(__typedContext.__subContextII, __rowSequence).asLongChunk();
        final IntChunk<? extends Values> __chunk__col__I = this.I.getPrevChunk(__typedContext.__subContextI, __rowSequence).asIntChunk();
        fillChunkHelper(true, __typedContext, __destination, __rowSequence, __chunk__col__II, __chunk__col__I);
    }

    private void fillChunkHelper(final boolean __usePrev, final FormulaFillContext __context,
            final WritableChunk<? super Values> __destination,
            final RowSequence __rowSequence, LongChunk<? extends Values> __chunk__col__II, IntChunk<? extends Values> __chunk__col__I) {
        final WritableLongChunk<? super Values> __typedDestination = __destination.asWritableLongChunk();
        try (final RowSet prev = __usePrev ? __rowSet.copyPrev() : null;
                final RowSet inverted = ((prev != null) ? prev : __rowSet).invert(__rowSequence.asRowSet())) {
            __context.__iChunk.setSize(0);
            inverted.forAllRowKeys(l -> __context.__iChunk.add(__intSize(l)));
            inverted.fillRowKeyChunk(__context.__iiChunk);
        }
        final int[] __chunkPosHolder = new int[] {0};
        if (__lazyResultCache != null) {
            __rowSequence.forAllRowKeys(k -> {
                final int __chunkPos = __chunkPosHolder[0]++;
                final int i = __context.__iChunk.get(__chunkPos);
                final long ii = __context.__iiChunk.get(__chunkPos);
                final Object __lazyKey = io.deephaven.engine.util.caching.C14nUtil.maybeMakeSmartKey(i, ii, __chunk__col__II.get(__chunkPos), __chunk__col__I.get(__chunkPos));
                __typedDestination.set(__chunkPos, (long)__lazyResultCache.computeIfAbsent(__lazyKey, __unusedKey -> applyFormulaPerItem(i, ii, __chunk__col__II.get(__chunkPos), __chunk__col__I.get(__chunkPos))));
            }
            );
        } else {
            __rowSequence.forAllRowKeys(k -> {
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
            throw new io.deephaven.engine.table.impl.select.FormulaEvaluationException("In formula: Value = " + "plus(plus(multiply(I, II), multiply(q.intValue(), ii)), II_.get(minus(i, 1)))", __e);
        }
    }

    @Override
    public FormulaFillContext makeFillContext(final int __chunkCapacity) {
        return new FormulaFillContext(__chunkCapacity);
    }

    private class FormulaFillContext implements io.deephaven.engine.table.impl.select.Formula.FillContext {
        private final WritableIntChunk<OrderedRowKeys> __iChunk;
        private final WritableLongChunk<OrderedRowKeys> __iiChunk;
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

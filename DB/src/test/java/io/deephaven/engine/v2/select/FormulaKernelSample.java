package io.deephaven.engine.v2.select;
// QueryLibrary internal version number: DEFAULT
import java.lang.*;
import java.util.*;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.tables.DataColumn;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.Param;
import io.deephaven.engine.tables.utils.ArrayUtils;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBPeriod;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.util.LongSizedDataStructure;
import static io.deephaven.engine.v2.select.ConditionFilter.FilterKernel;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.ByteChunk;
import io.deephaven.engine.v2.sources.chunk.CharChunk;
import io.deephaven.engine.v2.sources.chunk.Chunk;
import io.deephaven.engine.v2.sources.chunk.Context;
import io.deephaven.engine.v2.sources.chunk.DoubleChunk;
import io.deephaven.engine.v2.sources.chunk.FloatChunk;
import io.deephaven.engine.v2.sources.chunk.IntChunk;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.ObjectChunk;
import io.deephaven.engine.v2.sources.chunk.ShortChunk;
import io.deephaven.engine.v2.sources.chunk.WritableByteChunk;
import io.deephaven.engine.v2.sources.chunk.WritableCharChunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
import io.deephaven.engine.v2.sources.chunk.WritableDoubleChunk;
import io.deephaven.engine.v2.sources.chunk.WritableFloatChunk;
import io.deephaven.engine.v2.sources.chunk.WritableIntChunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.sources.chunk.WritableShortChunk;
import io.deephaven.engine.v2.utils.MutableRowSet;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.RowSetBuilderSequential;
import io.deephaven.engine.v2.utils.RowSetFactory;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.TrackingRowSet;
import io.deephaven.util.type.TypeUtils;
import java.lang.reflect.Array;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.LocalTime;
import static io.deephaven.base.string.cache.CompressedString.*;
import static io.deephaven.engine.tables.lang.DBLanguageFunctionUtil.*;
import static io.deephaven.engine.tables.utils.DBTimeUtils.*;
import static io.deephaven.engine.tables.utils.DBTimeZone.*;
import static io.deephaven.engine.tables.utils.WhereClause.*;
import static io.deephaven.engine.tables.verify.TableAssertions.*;
import static io.deephaven.engine.util.DBColorUtilImpl.*;
import static io.deephaven.engine.v2.sources.chunk.Attributes.*;
import static io.deephaven.gui.color.Color.*;
import static io.deephaven.libs.primitives.BinSearch.*;
import static io.deephaven.libs.primitives.BooleanPrimitives.*;
import static io.deephaven.libs.primitives.ByteNumericPrimitives.*;
import static io.deephaven.libs.primitives.BytePrimitives.*;
import static io.deephaven.libs.primitives.Casting.*;
import static io.deephaven.libs.primitives.CharacterPrimitives.*;
import static io.deephaven.libs.primitives.ComparePrimitives.*;
import static io.deephaven.libs.primitives.DoubleFpPrimitives.*;
import static io.deephaven.libs.primitives.DoubleNumericPrimitives.*;
import static io.deephaven.libs.primitives.DoublePrimitives.*;
import static io.deephaven.libs.primitives.FloatFpPrimitives.*;
import static io.deephaven.libs.primitives.FloatNumericPrimitives.*;
import static io.deephaven.libs.primitives.FloatPrimitives.*;
import static io.deephaven.libs.primitives.IntegerNumericPrimitives.*;
import static io.deephaven.libs.primitives.IntegerPrimitives.*;
import static io.deephaven.libs.primitives.LongNumericPrimitives.*;
import static io.deephaven.libs.primitives.LongPrimitives.*;
import static io.deephaven.libs.primitives.ObjectPrimitives.*;
import static io.deephaven.libs.primitives.PrimitiveParseUtil.*;
import static io.deephaven.libs.primitives.ShortNumericPrimitives.*;
import static io.deephaven.libs.primitives.ShortPrimitives.*;
import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.util.calendar.StaticCalendarMethods.*;

public class FormulaKernelSample implements io.deephaven.engine.v2.select.formula.FormulaKernel {
    public static final io.deephaven.engine.v2.select.formula.FormulaKernelFactory __FORMULA_KERNEL_FACTORY = FormulaKernelSample::new;

    private final io.deephaven.engine.tables.dbarrays.LongVector II_;
    private final java.lang.Integer q;

    public FormulaKernelSample(io.deephaven.engine.tables.dbarrays.Vector[] __dbArrays,
            io.deephaven.engine.tables.select.Param[] __params) {
        II_ = (io.deephaven.engine.tables.dbarrays.LongVector)__dbArrays[0];
        q = (java.lang.Integer)__params[0].getValue();
    }

    @Override
    public FormulaFillContext makeFillContext(final int __chunkCapacity) {
        return new FormulaFillContext(__chunkCapacity);
    }

    @Override
    public void applyFormulaChunk(io.deephaven.engine.v2.select.Formula.FillContext __context,
            final WritableChunk<? super Attributes.Values> __destination,
            Chunk<? extends Attributes.Values>[] __sources) {
        final WritableLongChunk<? super Attributes.Values> __typedDestination = __destination.asWritableLongChunk();
        final LongChunk<? extends Attributes.Values> __chunk__col__II = __sources[0].asLongChunk();
        final LongChunk<? extends Attributes.Values> __chunk__col__ii = __sources[1].asLongChunk();
        final IntChunk<? extends Attributes.Values> __chunk__col__I = __sources[2].asIntChunk();
        final IntChunk<? extends Attributes.Values> __chunk__col__i = __sources[3].asIntChunk();
        final int __size = __typedDestination.size();
        for (int __chunkPos = 0; __chunkPos < __size; ++__chunkPos) {
            __typedDestination.set(__chunkPos, applyFormulaPerItem(__chunk__col__II.get(__chunkPos), __chunk__col__ii.get(__chunkPos), __chunk__col__I.get(__chunkPos), __chunk__col__i.get(__chunkPos)));
        }
    }

    private long applyFormulaPerItem(long II, long ii, int I, int i) {
        try {
            return plus(plus(multiply(I, II), multiply(q.intValue(), ii)), II_.get(minus(i, 1)));
        } catch (java.lang.Exception __e) {
            throw new io.deephaven.engine.v2.select.FormulaEvaluationException("In formula: " + "plus(plus(multiply(I, II), multiply(q.intValue(), ii)), II_.get(minus(i, 1)))", __e);
        }
    }

    private class FormulaFillContext implements io.deephaven.engine.v2.select.Formula.FillContext {
        FormulaFillContext(int __chunkCapacity) {
        }
    }

}
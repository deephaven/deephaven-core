package io.deephaven.engine.table.impl.select;
// QueryLibrary internal version number: DEFAULT
import io.deephaven.chunk.attributes.*;
import io.deephaven.engine.rowset.chunkattributes.*;
import java.lang.*;
import java.util.*;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.chunk.BooleanChunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
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
import io.deephaven.engine.context.QueryScopeParam;
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
import io.deephaven.engine.table.Table;
import static io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.VectorConversions;
import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;
import static io.deephaven.base.string.cache.CompressedString.*;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;
import static io.deephaven.engine.table.impl.verify.TableAssertions.*;
import static io.deephaven.engine.util.ColorUtilImpl.*;
import static io.deephaven.function.Basic.*;
import static io.deephaven.function.BinSearch.*;
import static io.deephaven.function.BinSearchAlgo.*;
import static io.deephaven.function.Cast.*;
import static io.deephaven.function.Logic.*;
import static io.deephaven.function.Numeric.*;
import static io.deephaven.function.Parse.*;
import static io.deephaven.function.Random.*;
import static io.deephaven.function.Sort.*;
import static io.deephaven.gui.color.Color.*;

import static io.deephaven.time.DateTimeUtils.*;
import static io.deephaven.time.calendar.Calendars.*;
import static io.deephaven.time.calendar.StaticCalendarMethods.*;
import static io.deephaven.util.QueryConstants.*;

public class FilterKernelSample implements io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    private final int p1;
    private final float p2;
    private final java.lang.String p3;

    public FilterKernelSample(Table __table, RowSet __fullSet, QueryScopeParam... __params) {
        this.p1 = (java.lang.Integer) __params[0].getValue();
        this.p2 = (java.lang.Float) __params[1].getValue();
        this.p3 = (java.lang.String) __params[2].getValue();
    }
    @Override
    public Context getContext(int __maxChunkSize) {
        return new Context(__maxChunkSize);
    }
    
    @Override
    public LongChunk<OrderedRowKeys> filter(Context __context, LongChunk<OrderedRowKeys> __indices, Chunk... __inputChunks) {
        final ShortChunk __columnChunk0 = __inputChunks[0].asShortChunk();
        final DoubleChunk __columnChunk1 = __inputChunks[1].asDoubleChunk();
        final int __size = __indices.size();
        __context.resultChunk.setSize(0);
        for (int __my_i__ = 0; __my_i__ < __size; __my_i__++) {
            final short v1 =  (short)__columnChunk0.get(__my_i__);
            final double v2 =  (double)__columnChunk1.get(__my_i__);
            if ("foo".equals((plus(plus(plus(p1, p2), v1), v2)) + p3)) {
                __context.resultChunk.add(__indices.get(__my_i__));
            }
        }
        return __context.resultChunk;
    }
    
    @Override
    public int filter(final Context __context, final Chunk[] __inputChunks, final int __chunkSize, final WritableBooleanChunk<Values> __results) {
        final ShortChunk __columnChunk0 = __inputChunks[0].asShortChunk();
        final DoubleChunk __columnChunk1 = __inputChunks[1].asDoubleChunk();
        __results.setSize(__chunkSize);
        int __count = 0;
        for (int __my_i__ = 0; __my_i__ < __chunkSize; __my_i__++) {
            final short v1 =  (short)__columnChunk0.get(__my_i__);
            final double v2 =  (double)__columnChunk1.get(__my_i__);
            final boolean __newResult = "foo".equals((plus(plus(plus(p1, p2), v1), v2)) + p3);
            __results.set(__my_i__, __newResult);
            // count every true value
            __count += __newResult ? 1 : 0;
        }
        return __count;
    }
    
    @Override
    public int filterAnd(final Context __context, final Chunk[] __inputChunks, final int __chunkSize, final WritableBooleanChunk<Values> __results) {
        final ShortChunk __columnChunk0 = __inputChunks[0].asShortChunk();
        final DoubleChunk __columnChunk1 = __inputChunks[1].asDoubleChunk();
        __results.setSize(__chunkSize);
        int __count = 0;
        for (int __my_i__ = 0; __my_i__ < __chunkSize; __my_i__++) {
            final boolean __result = __results.get(__my_i__);
            if (!__result) {
                // already false, no need to compute or increment the count
                continue;
            }
            final short v1 =  (short)__columnChunk0.get(__my_i__);
            final double v2 =  (double)__columnChunk1.get(__my_i__);
            final boolean __newResult = "foo".equals((plus(plus(plus(p1, p2), v1), v2)) + p3);
            __results.set(__my_i__, __newResult);
            __results.set(__my_i__, __newResult);
            // increment the count if the new result is TRUE
            __count += __newResult ? 1 : 0;
        }
        return __count;
    }
}

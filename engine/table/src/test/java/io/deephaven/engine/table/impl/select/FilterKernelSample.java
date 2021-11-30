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

public class FilterKernelSample implements io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    private final int p1;
    private final float p2;
    private final java.lang.String p3;

    public FilterKernelSample(Table table, RowSet fullSet, QueryScopeParam... params) {
        this.p1 = (java.lang.Integer) params[0].getValue();
        this.p2 = (java.lang.Float) params[1].getValue();
        this.p3 = (java.lang.String) params[2].getValue();
    }
    @Override
    public Context getContext(int maxChunkSize) {
        return new Context(maxChunkSize);
    }
    
    @Override
    public LongChunk<OrderedRowKeys> filter(Context context, LongChunk<OrderedRowKeys> indices, Chunk... inputChunks) {
        final ShortChunk __columnChunk0 = inputChunks[0].asShortChunk();
        final DoubleChunk __columnChunk1 = inputChunks[1].asDoubleChunk();
        final int size = indices.size();
        context.resultChunk.setSize(0);
        for (int __my_i__ = 0; __my_i__ < size; __my_i__++) {
            final short v1 =  (short)__columnChunk0.get(__my_i__);
            final double v2 =  (double)__columnChunk1.get(__my_i__);
            if ("foo".equals((plus(plus(plus(p1, p2), v1), v2))+p3)) {
                context.resultChunk.add(indices.get(__my_i__));
            }
        }
        return context.resultChunk;
    }
}
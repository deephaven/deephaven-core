package io.deephaven.engine.table.impl.select;
// QueryLibrary internal version number: DEFAULT
import java.lang.*;
import java.util.*;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ByteChunk;
import io.deephaven.engine.chunk.CharChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.DoubleChunk;
import io.deephaven.engine.chunk.FloatChunk;
import io.deephaven.engine.chunk.IntChunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.ShortChunk;
import io.deephaven.engine.chunk.WritableByteChunk;
import io.deephaven.engine.chunk.WritableCharChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableDoubleChunk;
import io.deephaven.engine.chunk.WritableFloatChunk;
import io.deephaven.engine.chunk.WritableIntChunk;
import io.deephaven.engine.chunk.WritableLongChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.chunk.WritableShortChunk;
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
import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.time.DateTimeUtils;
import io.deephaven.engine.time.Period;
import io.deephaven.engine.vector.VectorConversions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import java.lang.reflect.Array;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.LocalTime;
import static io.deephaven.base.string.cache.CompressedString.*;
import static io.deephaven.engine.chunk.Attributes.*;
import static io.deephaven.engine.function.BinSearch.*;
import static io.deephaven.engine.function.BooleanPrimitives.*;
import static io.deephaven.engine.function.ByteNumericPrimitives.*;
import static io.deephaven.engine.function.BytePrimitives.*;
import static io.deephaven.engine.function.Casting.*;
import static io.deephaven.engine.function.CharacterPrimitives.*;
import static io.deephaven.engine.function.ComparePrimitives.*;
import static io.deephaven.engine.function.DoubleFpPrimitives.*;
import static io.deephaven.engine.function.DoubleNumericPrimitives.*;
import static io.deephaven.engine.function.DoublePrimitives.*;
import static io.deephaven.engine.function.FloatFpPrimitives.*;
import static io.deephaven.engine.function.FloatNumericPrimitives.*;
import static io.deephaven.engine.function.FloatPrimitives.*;
import static io.deephaven.engine.function.IntegerNumericPrimitives.*;
import static io.deephaven.engine.function.IntegerPrimitives.*;
import static io.deephaven.engine.function.LongNumericPrimitives.*;
import static io.deephaven.engine.function.LongPrimitives.*;
import static io.deephaven.engine.function.ObjectPrimitives.*;
import static io.deephaven.engine.function.PrimitiveParseUtil.*;
import static io.deephaven.engine.function.ShortNumericPrimitives.*;
import static io.deephaven.engine.function.ShortPrimitives.*;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;
import static io.deephaven.engine.table.impl.verify.TableAssertions.*;
import static io.deephaven.engine.time.DateTimeUtils.*;
import static io.deephaven.engine.time.TimeZone.*;
import static io.deephaven.engine.time.calendar.StaticCalendarMethods.*;
import static io.deephaven.engine.util.ColorUtilImpl.*;
import static io.deephaven.gui.color.Color.*;
import static io.deephaven.util.QueryConstants.*;

public class FilterKernelArraySample implements io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    // Array Column Variables
    private final io.deephaven.engine.vector.DoubleVector v2_;
    private final io.deephaven.engine.vector.ShortVector v1_;


    public FilterKernelArraySample(Table table, RowSet fullSet, QueryScopeParam... params) {

        // Array Column Variables
        v2_ = new io.deephaven.engine.table.impl.vector.DoubleVectorColumnWrapper(table.getColumnSource("v2"), fullSet);
        v1_ = new io.deephaven.engine.table.impl.vector.ShortVectorColumnWrapper(table.getColumnSource("v1"), fullSet);
    }
    @Override
    public Context getContext(int maxChunkSize) {
        return new Context(maxChunkSize);
    }
    
    @Override
    public LongChunk<OrderedRowKeys> filter(Context context, LongChunk<OrderedRowKeys> indices, Chunk... inputChunks) {
        final int size = indices.size();
        context.resultChunk.setSize(0);
        for (int __my_i__ = 0; __my_i__ < size; __my_i__++) {
            if (eq(v1_.size(), v2_.size())) {
                context.resultChunk.add(indices.get(__my_i__));
            }
        }
        return context.resultChunk;
    }
}
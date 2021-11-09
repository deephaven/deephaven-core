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

public class FilterKernelArraySample implements io.deephaven.engine.v2.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    // Array Column Variables
    private final io.deephaven.engine.tables.dbarrays.DoubleVector v2_;
    private final io.deephaven.engine.tables.dbarrays.ShortVector v1_;


    public FilterKernelArraySample(Table table, TrackingRowSet fullSet, Param... params) {

        // Array Column Variables
        v2_ = new io.deephaven.engine.v2.dbarrays.DoubleVectorColumnWrapper(table.getColumnSource("v2"), fullSet);
        v1_ = new io.deephaven.engine.v2.dbarrays.ShortVectorColumnWrapper(table.getColumnSource("v1"), fullSet);
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
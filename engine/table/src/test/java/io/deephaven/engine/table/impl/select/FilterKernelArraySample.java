package io.deephaven.engine.table.impl.select;
// QueryLibrary internal version number: DEFAULT
import io.deephaven.engine.rowset.chunkattributes.*;
import java.lang.*;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import static io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;

public class FilterKernelArraySample implements io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    // Array Column Variables
    private final io.deephaven.vector.DoubleVector v2_;
    private final io.deephaven.vector.ShortVector v1_;


    public FilterKernelArraySample(Table __table, RowSet __fullSet, QueryScopeParam... __params) {

        // Array Column Variables
        v2_ = new io.deephaven.engine.table.impl.vector.DoubleVectorColumnWrapper(__table.getColumnSource("v2"), __fullSet);
        v1_ = new io.deephaven.engine.table.impl.vector.ShortVectorColumnWrapper(__table.getColumnSource("v1"), __fullSet);
    }
    @Override
    public Context getContext(int __maxChunkSize) {
        return new Context(__maxChunkSize);
    }
    
    @Override
    public LongChunk<OrderedRowKeys> filter(Context __context, LongChunk<OrderedRowKeys> __indices, Chunk... __inputChunks) {
        final int __size = __indices.size();
        __context.resultChunk.setSize(0);
        for (int __my_i__ = 0; __my_i__ < __size; __my_i__++) {
            if (eq(v1_.size(), v2_.size())) {
                __context.resultChunk.add(__indices.get(__my_i__));
            }
        }
        return __context.resultChunk;
    }
}
package io.deephaven.engine.v2.select;
// QueryLibrary internal version number: DEFAULT
import java.lang.*;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.Param;

import static io.deephaven.engine.v2.select.ConditionFilter.FilterKernel;

import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.v2.utils.TrackingRowSet;
import io.deephaven.engine.vector.DoubleVector;
import io.deephaven.engine.vector.ShortVector;

import static io.deephaven.engine.tables.lang.LanguageFunctionUtil.*;
import static io.deephaven.engine.chunk.Attributes.*;

public class FilterKernelArraySample implements io.deephaven.engine.v2.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    // Array Column Variables
    private final DoubleVector v2_;
    private final ShortVector v1_;


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
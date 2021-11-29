package io.deephaven.engine.table.impl.select;
// QueryLibrary internal version number: DEFAULT
import java.lang.*;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import static io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.rowset.chunkattributes.OrderedRowKeys;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;

public class FilterKernelArraySample implements io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    // Array Column Variables
    private final io.deephaven.vector.DoubleVector v2_;
    private final io.deephaven.vector.ShortVector v1_;


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
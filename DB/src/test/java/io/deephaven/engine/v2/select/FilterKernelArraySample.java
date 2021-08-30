package io.deephaven.engine.v2.select;
// QueryLibrary internal version number: DEFAULT
import java.lang.*;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.Param;

import static io.deephaven.engine.v2.select.ConditionFilter.FilterKernel;

import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.rowset.Index;

import static io.deephaven.engine.tables.lang.DBLanguageFunctionUtil.*;
import static io.deephaven.engine.structures.chunk.Attributes.*;

public class FilterKernelArraySample implements io.deephaven.engine.v2.select.ConditionFilter.FilterKernel<FilterKernel.Context>{


    // Array Column Variables
    private final io.deephaven.engine.structures.vector.DbDoubleArray v2_;
    private final io.deephaven.engine.structures.vector.DbShortArray v1_;


    public FilterKernelArraySample(Table table, Index fullSet, Param... params) {

        // Array Column Variables
        v2_ = new io.deephaven.engine.v2.dbarrays.DbDoubleArrayColumnWrapper(table.getColumnSource("v2"), fullSet);
        v1_ = new io.deephaven.engine.v2.dbarrays.DbShortArrayColumnWrapper(table.getColumnSource("v1"), fullSet);
    }
    @Override
    public Context getContext(int maxChunkSize) {
        return new Context(maxChunkSize);
    }
    
    @Override
    public LongChunk<OrderedKeyIndices> filter(Context context, LongChunk<OrderedKeyIndices> indices, Chunk... inputChunks) {
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
package io.deephaven.engine.table.impl.select;
// QueryLibrary internal version number: DEFAULT
import io.deephaven.engine.rowset.chunkattributes.*;
import java.lang.*;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import static io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel;
import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;

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
            if ("foo".equals((plus(plus(plus(p1, p2), v1), v2))+p3)) {
                __context.resultChunk.add(__indices.get(__my_i__));
            }
        }
        return __context.resultChunk;
    }
}
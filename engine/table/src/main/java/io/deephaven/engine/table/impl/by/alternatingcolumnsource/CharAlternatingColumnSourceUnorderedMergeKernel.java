package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import static io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource.ALTERNATE_SWITCH_MASK;

public class CharAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static CharAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new CharAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private CharAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableCharChunk<? super Values> destAsChar = dest.asWritableCharChunk();
        final CharChunk<? super Values> srcAsChar = src.asCharChunk();

        // now merge them together
        int mainPosition = 0;
        destAsChar.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsChar.set(ii, srcAsChar.get(mainPosition++));
            } else {
                destAsChar.set(ii, srcAsChar.get(alternatePosition++));
            }
        }
    }
}

/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAlternatingColumnSourceUnorderedMergeKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import static io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource.ALTERNATE_SWITCH_MASK;

public class ObjectAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static ObjectAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new ObjectAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private ObjectAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableObjectChunk<Object, ? super Values> destAsObject = dest.asWritableObjectChunk();
        final ObjectChunk<Object, ? super Values> srcAsObject = src.asObjectChunk();

        // now merge them together
        int mainPosition = 0;
        destAsObject.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsObject.set(ii, srcAsObject.get(mainPosition++));
            } else {
                destAsObject.set(ii, srcAsObject.get(alternatePosition++));
            }
        }
    }
}

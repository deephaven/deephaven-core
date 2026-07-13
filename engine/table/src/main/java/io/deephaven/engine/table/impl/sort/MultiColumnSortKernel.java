//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.Context;

/**
 * The MultiColumnSortKernel sorts a set of parallel value chunks — comparing each column in turn, as a lexicographic
 * sort key — together with a parallel LongChunk of row keys, in a single pass rather than sorting one column at a time
 * with run finding in between.
 */
public interface MultiColumnSortKernel<PERMUTE_VALUES_ATTR extends Any> extends Context {
    /**
     * Sort the values in the valuesToSort chunks, comparing each column in turn, permuting the valuesToPermute chunk in
     * the same way.
     *
     * @param valuesToPermute the row keys (or other values) permuted alongside the sort key columns
     * @param valuesToSort one chunk per sort key column, each of the specialized writable chunk type this kernel was
     *        created for; all chunks must have the same size as valuesToPermute
     */
    void sort(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute, WritableChunk<? extends Any>[] valuesToSort);
}

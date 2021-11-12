/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.chunk.LongChunk;

public class RowSequenceRowKeysChunkImplTest extends RowSequenceTestBase {

    @Override
    protected RowSequence create(long... values) {
        return RowSequenceUtil.wrapRowKeysChunkAsRowSequence(LongChunk.chunkWrap(values));
    }
}

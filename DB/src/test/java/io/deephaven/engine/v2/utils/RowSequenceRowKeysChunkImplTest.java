/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceUtil;
import io.deephaven.engine.v2.sources.chunk.LongChunk;

public class RowSequenceRowKeysChunkImplTest extends RowSequenceTestBase {

    @Override
    protected RowSequence create(long... values) {
        return RowSequenceUtil.wrapRowKeysChunkAsRowSequence(LongChunk.chunkWrap(values));
    }
}

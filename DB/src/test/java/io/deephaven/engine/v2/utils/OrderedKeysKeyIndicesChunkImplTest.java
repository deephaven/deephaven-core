/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.chunk.LongChunk;

public class OrderedKeysKeyIndicesChunkImplTest extends OrderedKeysTestBase {

    @Override
    protected OrderedKeys create(long... values) {
        return OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(LongChunk.chunkWrap(values));
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.LongChunk;

public class OrderedKeysKeyIndicesChunkImplTest extends OrderedKeysTestBase {

    @Override
    protected OrderedKeys create(long... values) {
        return OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(LongChunk.chunkWrap(values));
    }
}

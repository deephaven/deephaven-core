//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.NotNull;

/**
 * A row redirection which implements the identity transformation.
 */
public class IdentityRowRedirection implements RowRedirection {
    public static IdentityRowRedirection INSTANCE = new IdentityRowRedirection();

    // static use only
    private IdentityRowRedirection() {}

    @Override
    public long get(long outerRowKey) {
        return outerRowKey;
    }

    @Override
    public long getPrev(long outerRowKey) {
        return outerRowKey;
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext() {
            @Override
            public boolean supportsUnboundedFill() {
                return true;
            }
        };
    }

    @Override
    public void fillChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(outerRowKeys.intSize());
        outerRowKeys.fillRowKeyChunk(innerRowKeysTyped);
    }

    @Override
    public void fillPrevChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        fillChunk(fillContext, innerRowKeys, outerRowKeys);
    }

    @Override
    public boolean ascendingMapping() {
        return true;
    }


    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}

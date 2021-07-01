/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharArrayExpansionKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.barrage.chunk.array;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.BooleanChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.sources.chunk.sized.SizedBooleanChunk;

public class BooleanArrayExpansionKernel implements ArrayExpansionKernel {
    private final static boolean[] ZERO_LEN_ARRAY = new boolean[0];
    public final static BooleanArrayExpansionKernel INSTANCE = new BooleanArrayExpansionKernel();

    @Override
    public <T, A extends Attributes.Any> WritableChunk<A> expand(final ObjectChunk<T, A> source, final WritableIntChunk<Attributes.ChunkPositions> perElementLengthDest) {
        if (source.size() == 0) {
            perElementLengthDest.setSize(0);
            return WritableBooleanChunk.makeWritableChunk(0);
        }

        final ObjectChunk<boolean[], A> typedSource = source.asObjectChunk();
        final SizedBooleanChunk<A> resultWrapper = new SizedBooleanChunk<>();

        int lenWritten = 0;
        perElementLengthDest.setSize(source.size() + 1);
        for (int i = 0; i < typedSource.size(); ++i) {
            final boolean[] row = typedSource.get(i);
            final int len = row == null ? 0 : row.length;
            perElementLengthDest.set(i, lenWritten);
            final WritableBooleanChunk<A> result = resultWrapper.ensureCapacityPreserve(lenWritten + len);
            for (int j = 0; j < len; ++j) {
                result.set(lenWritten + j, row[j]);
            }
            lenWritten += len;
            result.setSize(lenWritten);
        }
        perElementLengthDest.set(typedSource.size(), lenWritten);

        return resultWrapper.get();
    }

    @Override
    public <T, A extends Attributes.Any> WritableObjectChunk<T, A> contract(
            final Chunk<A> source, final IntChunk<Attributes.ChunkPositions> perElementLengthDest) {
        if (perElementLengthDest.size() == 0) {
            return WritableObjectChunk.makeWritableChunk(0);
        }

        final BooleanChunk<A> typedSource = source.asBooleanChunk();
        final WritableObjectChunk<Object, A> result = WritableObjectChunk.makeWritableChunk(perElementLengthDest.size() - 1);

        int lenRead = 0;
        for (int i = 0; i < result.size(); ++i) {
            final int ROW_LEN = perElementLengthDest.get(i + 1) - perElementLengthDest.get(i);
            if (ROW_LEN == 0) {
                result.set(i, ZERO_LEN_ARRAY);
            } else {
                final boolean[] row = new boolean[ROW_LEN];
                for (int j = 0; j < ROW_LEN; ++j) {
                    row[j] = typedSource.get(lenRead + j);
                }
                lenRead += ROW_LEN;
                result.set(i, row);
            }
        }

        //noinspection unchecked
        return (WritableObjectChunk<T, A>)result;
    }
}

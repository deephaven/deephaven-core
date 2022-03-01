/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.chunk.vector;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.sized.SizedCharChunk;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.vector.Vector;

public class CharVectorExpansionKernel implements VectorExpansionKernel {
    private final static CharVector ZERO_LEN_VECTOR = new CharVectorDirect();
    public final static CharVectorExpansionKernel INSTANCE = new CharVectorExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            final ObjectChunk<Vector<?>, A> source, final WritableIntChunk<ChunkPositions> perElementLengthDest) {
        if (source.size() == 0) {
            perElementLengthDest.setSize(0);
            return WritableCharChunk.makeWritableChunk(0);
        }

        final ObjectChunk<CharVector, A> typedSource = source.asObjectChunk();
        final SizedCharChunk<A> resultWrapper = new SizedCharChunk<>();

        int lenWritten = 0;
        perElementLengthDest.setSize(source.size() + 1);
        for (int i = 0; i < typedSource.size(); ++i) {
            final CharVector row = typedSource.get(i);
            final int len = row == null ? 0 : row.intSize("CharVectorExpansionKernel");
            perElementLengthDest.set(i, lenWritten);
            final WritableCharChunk<A> result = resultWrapper.ensureCapacityPreserve(lenWritten + len);
            for (int j = 0; j < len; ++j) {
                result.set(lenWritten + j, row.get(j));
            }
            lenWritten += len;
            result.setSize(lenWritten);
        }
        perElementLengthDest.set(typedSource.size(), lenWritten);

        return resultWrapper.get();
    }

    @Override
    public <A extends Any> WritableObjectChunk<Vector<?>, A> contract(
            final Chunk<A> source, final IntChunk<ChunkPositions> perElementLengthDest) {
        if (perElementLengthDest.size() == 0) {
            return WritableObjectChunk.makeWritableChunk(0);
        }

        final CharChunk<A> typedSource = source.asCharChunk();
        final WritableObjectChunk<Vector<?>, A> result =
                WritableObjectChunk.makeWritableChunk(perElementLengthDest.size() - 1);

        int lenRead = 0;
        for (int i = 0; i < result.size(); ++i) {
            final int ROW_LEN = perElementLengthDest.get(i + 1) - perElementLengthDest.get(i);
            if (ROW_LEN == 0) {
                result.set(i, ZERO_LEN_VECTOR);
            } else {
                final char[] row = new char[ROW_LEN];
                for (int j = 0; j < ROW_LEN; ++j) {
                    row[j] = typedSource.get(lenRead + j);
                }
                lenRead += ROW_LEN;
                result.set(i, new CharVectorDirect(row));
            }
        }

        return result;
    }
}

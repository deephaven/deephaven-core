//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;

/**
 * Fills a pre-allocated {@link WritableIntChunk} with dictionary indices for one column batch. One implementation per
 * source {@link ChunkType}; avoids boxing and per-element ChunkType dispatch in the hot path.
 *
 * <p>
 * The chunk is expected to be pre-sized to the logical row count. After {@code fillIndexChunk} returns, every output
 * position corresponds to exactly one logical row: {@code QueryConstants.NULL_INT} for null rows (in
 * non-deephaven-nulls mode) and a non-negative dictionary index for non-null rows.
 */
interface DictionaryIndexKernel {

    void fillIndexChunk(
            Chunk<Values> source,
            RowSet subset,
            BarrageOptions options,
            DictionaryWriterState state,
            WritableIntChunk<Values> out);

    static DictionaryIndexKernel make(final ChunkType valuesChunkType) {
        switch (valuesChunkType) {
            case Byte:
                return ByteDictionaryIndexKernel.INSTANCE;
            case Char:
                return CharDictionaryIndexKernel.INSTANCE;
            case Short:
                return ShortDictionaryIndexKernel.INSTANCE;
            case Int:
                return IntDictionaryIndexKernel.INSTANCE;
            case Long:
                return LongDictionaryIndexKernel.INSTANCE;
            case Float:
                return FloatDictionaryIndexKernel.INSTANCE;
            case Double:
                return DoubleDictionaryIndexKernel.INSTANCE;
            default:
                return ObjectDictionaryIndexKernel.INSTANCE;
        }
    }
}

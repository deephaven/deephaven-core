package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

interface AlternatingColumnSourceUnorderedMergeKernel {
    static AlternatingColumnSourceUnorderedMergeKernel getInstance(ChunkType chunkType) {
        switch (chunkType) {
            case Boolean:
                break;
            case Char:
                return CharAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
            case Byte:
                return ByteAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
            case Short:
                return ShortAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
            case Int:
                return IntAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
            case Long:
                return LongAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
            case Float:
                return FloatAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
            case Double:
                return DoubleAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
            case Object:
                return ObjectAlternatingColumnSourceUnorderedMergeKernel.INSTANCE;
        }
        throw new IllegalStateException();
    }

    void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys,
            Chunk<? super Values> src, int alternatePosition);
}

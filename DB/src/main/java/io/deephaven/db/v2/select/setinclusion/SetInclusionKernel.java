package io.deephaven.db.v2.select.setinclusion;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;

import java.util.Collection;

public interface SetInclusionKernel {
    void matchValues(Chunk<Attributes.Values> values, WritableBooleanChunk matches);

    static SetInclusionKernel makeKernel(ChunkType type, Collection<Object> values,
        boolean inclusion) {
        switch (type) {
            case Object:
                return new ObjectSetInclusionKernel(values, inclusion);
            case Char:
                return new CharSetInclusionKernel(values, inclusion);
            case Byte:
                return new ByteSetInclusionKernel(values, inclusion);
            case Short:
                return new ShortSetInclusionKernel(values, inclusion);
            case Int:
                return new IntSetInclusionKernel(values, inclusion);
            case Long:
                return new LongSetInclusionKernel(values, inclusion);
            case Double:
                return new DoubleSetInclusionKernel(values, inclusion);
            case Float:
                return new FloatSetInclusionKernel(values, inclusion);
            default:
                throw new UnsupportedOperationException();
        }
    }
}

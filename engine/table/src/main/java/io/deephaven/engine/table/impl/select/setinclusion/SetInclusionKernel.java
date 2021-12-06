package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.Collection;

public interface SetInclusionKernel {
    void matchValues(Chunk<Values> values, WritableBooleanChunk matches);

    static SetInclusionKernel makeKernel(ChunkType type, Collection<Object> values, boolean inclusion) {
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
